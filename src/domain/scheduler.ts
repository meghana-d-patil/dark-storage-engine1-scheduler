import Database from 'better-sqlite3';
import { EventEmitter } from 'node:events';
import { TaskRecord } from '@dark-storage/shared';
import { tryAcquireAll, sweepExpiredLocks } from './lockManager.js';
import { calculateEffectiveScore } from './queue.js';
import { executeTask, TaskExecutorFn, defaultHttpExecutor } from './executor.js';

export interface SchedulerOptions {
  maxConcurrency?: number;
  tickIntervalMs?: number;
  sweepIntervalMs?: number;
  executor?: TaskExecutorFn;
}

export class Scheduler {
  private db: Database.Database;
  private maxConcurrency: number;
  private tickIntervalMs: number;
  private sweepIntervalMs: number;
  private executor: TaskExecutorFn;

  private runningTasks = new Map<string, TaskRecord>();
  private tickTimer: NodeJS.Timeout | null = null;
  private sweepTimer: NodeJS.Timeout | null = null;
  private emitter = new EventEmitter();
  private stopped = false;

  constructor(db: Database.Database, options: SchedulerOptions = {}) {
    this.db = db;
    this.maxConcurrency = options.maxConcurrency ?? 4;
    this.tickIntervalMs = options.tickIntervalMs ?? 100;
    this.sweepIntervalMs = options.sweepIntervalMs ?? 5000;
    this.executor = options.executor ?? defaultHttpExecutor;
  }

  public setExecutor(executor: TaskExecutorFn): void {
    this.executor = executor;
  }

  public start(): void {
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

  public stop(): void {
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

  public recoverCrash(): void {
    const nowIso = new Date().toISOString();
    const recoverTx = this.db.transaction(() => {
      // 1. Delete all locks
      this.db.prepare('DELETE FROM locks').run();

      // 2. Reset running tasks to QUEUED
      const running = this.db.prepare("SELECT id, attempts FROM tasks WHERE status = 'RUNNING'").all() as { id: string; attempts: number }[];
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

  public sweep(now: Date = new Date()): number {
    const runningIds = new Set(this.runningTasks.keys());
    return sweepExpiredLocks(this.db, now, runningIds);
  }

  public tick(now: Date = new Date()): void {
    if (this.stopped) return;

    let availableSlots = this.maxConcurrency - this.runningTasks.size;
    if (availableSlots <= 0) return;

    const nowIso = now.toISOString();
    const queuedTasks = this.db.prepare(`
      SELECT * FROM tasks
      WHERE status = 'QUEUED' AND (next_attempt_at IS NULL OR next_attempt_at <= ?)
    `).all(nowIso) as TaskRecord[];

    if (queuedTasks.length === 0) return;

    // Calculate effective scores
    const scored = queuedTasks.map((task) => ({
      task,
      score: calculateEffectiveScore(task.priority, task.created_at, now)
    }));

    // Sort: effective_score DESC, created_at ASC, id ASC
    scored.sort((a, b) => {
      if (b.score !== a.score) return b.score - a.score;
      if (a.task.created_at !== b.task.created_at) {
        return a.task.created_at.localeCompare(b.task.created_at);
      }
      return a.task.id.localeCompare(b.task.id);
    });

    const blocked = new Set<string>();

    for (const item of scored) {
      if (availableSlots <= 0) break;
      const { task } = item;
      const resources: string[] = JSON.parse(task.resources_json);

      // Head-of-line protection: if any resource is already in blocked set, add all resources to blocked and skip
      if (resources.some((r) => blocked.has(r))) {
        for (const r of resources) blocked.add(r);
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
      } else {
        for (const r of resources) blocked.add(r);
      }
    }
  }

  public enqueueImmediate(): void {
    setImmediate(() => this.tick());
  }

  public async waitForTask(taskId: string, timeoutMs: number = 5000): Promise<TaskRecord | null> {
    const checkTerminal = (): TaskRecord | null => {
      const task = this.db.prepare('SELECT * FROM tasks WHERE id = ?').get(taskId) as TaskRecord | undefined;
      if (!task) return null;
      if (task.status === 'SUCCEEDED' || task.status === 'FAILED') {
        return task;
      }
      return null;
    };

    const terminal = checkTerminal();
    if (terminal) return terminal;

    return new Promise((resolve) => {
      let timer: NodeJS.Timeout | null = null;

      const handler = () => {
        const t = checkTerminal();
        if (t) {
          if (timer) clearTimeout(timer);
          this.emitter.off(`task:${taskId}`, handler);
          resolve(t);
        }
      };

      this.emitter.on(`task:${taskId}`, handler);

      timer = setTimeout(() => {
        this.emitter.off(`task:${taskId}`, handler);
        const finalCheck = this.db.prepare('SELECT * FROM tasks WHERE id = ?').get(taskId) as TaskRecord | undefined;
        resolve(finalCheck || null);
      }, timeoutMs);
    });
  }
}

let schedulerInstance: Scheduler | null = null;

export function getScheduler(db?: Database.Database): Scheduler {
  if (!schedulerInstance) {
    if (!db) {
      throw new Error('Scheduler not initialized and no db provided');
    }
    schedulerInstance = new Scheduler(db);
  }
  return schedulerInstance;
}

export function setSchedulerInstance(scheduler: Scheduler | null): void {
  schedulerInstance = scheduler;
}
