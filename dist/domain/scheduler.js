import { EventEmitter } from 'node:events';
import { tryAcquireAll, sweepExpiredLocks } from './lockManager.js';
import { calculateEffectiveScore } from './queue.js';
import { executeTask, defaultHttpExecutor } from './executor.js';
export class Scheduler {
    db;
    maxConcurrency;
    tickIntervalMs;
    sweepIntervalMs;
    executor;
    runningTasks = new Map();
    tickTimer = null;
    sweepTimer = null;
    emitter = new EventEmitter();
    stopped = false;
    constructor(db, options = {}) {
        this.db = db;
        this.maxConcurrency = options.maxConcurrency ?? 4;
        this.tickIntervalMs = options.tickIntervalMs ?? 100;
        this.sweepIntervalMs = options.sweepIntervalMs ?? 5000;
        this.executor = options.executor ?? defaultHttpExecutor;
    }
    setExecutor(executor) {
        this.executor = executor;
    }
    start() {
        this.stopped = false;
        this.recoverCrash();
        // Start background tick loop
        this.tickTimer = setInterval(() => {
            this.tick();
        }, this.tickIntervalMs);
        // Start sweeper loop
        this.sweepTimer = setInterval(() => {
            this.sweep();
        }, this.sweepIntervalMs);
        // Immediate first tick
        this.tick();
    }
    stop() {
        this.stopped = true;
        if (this.tickTimer) {
            clearInterval(this.tickTimer);
            this.tickTimer = null;
        }
        if (this.sweepTimer) {
            clearInterval(this.sweepTimer);
            this.sweepTimer = null;
        }
    }
    recoverCrash() {
        const nowIso = new Date().toISOString();
        const recoverTx = this.db.transaction(() => {
            // 1. Delete all locks
            this.db.prepare('DELETE FROM locks').run();
            // 2. Reset running tasks to QUEUED
            const running = this.db.prepare("SELECT id, attempts FROM tasks WHERE status = 'RUNNING'").all();
            for (const row of running) {
                const nextAttempts = Math.max(0, row.attempts - 1);
                this.db.prepare(`
          UPDATE tasks
          SET status = 'QUEUED', attempts = ?, started_at = NULL
          WHERE id = ?
        `).run(nextAttempts, row.id);
                this.db.prepare(`
          INSERT INTO task_events (task_id, at, event, details_json)
          VALUES (?, ?, 'RECOVERED_AFTER_RESTART', NULL)
        `).run(row.id, nowIso);
            }
        });
        recoverTx();
    }
    sweep(now = new Date()) {
        const runningIds = new Set(this.runningTasks.keys());
        return sweepExpiredLocks(this.db, now, runningIds);
    }
    tick(now = new Date()) {
        if (this.stopped)
            return;
        let availableSlots = this.maxConcurrency - this.runningTasks.size;
        if (availableSlots <= 0)
            return;
        const nowIso = now.toISOString();
        const queuedTasks = this.db.prepare(`
      SELECT * FROM tasks
      WHERE status = 'QUEUED' AND (next_attempt_at IS NULL OR next_attempt_at <= ?)
    `).all(nowIso);
        if (queuedTasks.length === 0)
            return;
        // Calculate effective scores
        const scored = queuedTasks.map((task) => ({
            task,
            score: calculateEffectiveScore(task.priority, task.created_at, now)
        }));
        // Sort: effective_score DESC, created_at ASC, id ASC
        scored.sort((a, b) => {
            if (b.score !== a.score)
                return b.score - a.score;
            if (a.task.created_at !== b.task.created_at) {
                return a.task.created_at.localeCompare(b.task.created_at);
            }
            return a.task.id.localeCompare(b.task.id);
        });
        const blocked = new Set();
        for (const item of scored) {
            if (availableSlots <= 0)
                break;
            const { task } = item;
            const resources = JSON.parse(task.resources_json);
            // Head-of-line protection: if any resource is already in blocked set, add all resources to blocked and skip
            if (resources.some((r) => blocked.has(r))) {
                for (const r of resources)
                    blocked.add(r);
                continue;
            }
            if (tryAcquireAll(this.db, task, now)) {
                availableSlots--;
                this.runningTasks.set(task.id, task);
                // Async execution, not awaited here
                executeTask(this.db, task, this.executor)
                    .catch((err) => {
                    console.error(`Unexpected task execution failure for ${task.id}:`, err);
                })
                    .finally(() => {
                    this.runningTasks.delete(task.id);
                    this.emitter.emit(`task:${task.id}`);
                    this.emitter.emit('task:any');
                    this.tick();
                });
            }
            else {
                for (const r of resources)
                    blocked.add(r);
            }
        }
    }
    enqueueImmediate() {
        setImmediate(() => this.tick());
    }
    async waitForTask(taskId, timeoutMs = 5000) {
        const checkTerminal = () => {
            const task = this.db.prepare('SELECT * FROM tasks WHERE id = ?').get(taskId);
            if (!task)
                return null;
            if (task.status === 'SUCCEEDED' || task.status === 'FAILED') {
                return task;
            }
            return null;
        };
        const terminal = checkTerminal();
        if (terminal)
            return terminal;
        return new Promise((resolve) => {
            let timer = null;
            const handler = () => {
                const t = checkTerminal();
                if (t) {
                    if (timer)
                        clearTimeout(timer);
                    this.emitter.off(`task:${taskId}`, handler);
                    resolve(t);
                }
            };
            this.emitter.on(`task:${taskId}`, handler);
            timer = setTimeout(() => {
                this.emitter.off(`task:${taskId}`, handler);
                const finalCheck = this.db.prepare('SELECT * FROM tasks WHERE id = ?').get(taskId);
                resolve(finalCheck || null);
            }, timeoutMs);
        });
    }
}
let schedulerInstance = null;
export function getScheduler(db) {
    if (!schedulerInstance) {
        if (!db) {
            throw new Error('Scheduler not initialized and no db provided');
        }
        schedulerInstance = new Scheduler(db);
    }
    return schedulerInstance;
}
export function setSchedulerInstance(scheduler) {
    schedulerInstance = scheduler;
}
//# sourceMappingURL=scheduler.js.map