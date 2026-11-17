import { Router } from 'express';
import crypto from 'node:crypto';
import { createTaskSchema } from '@dark-storage/shared';
import { getSchedulerDb } from '../db/connection.js';
import { getScheduler } from '../domain/scheduler.js';
import { calculateEffectiveScore } from '../domain/queue.js';
export const tasksRouter = Router();
function formatTaskDto(task) {
    let result = undefined;
    if (task.result_json) {
        try {
            result = JSON.parse(task.result_json);
        }
        catch {
            result = task.result_json;
        }
    }
    let error = undefined;
    if (task.error_code) {
        let details = undefined;
        if (task.error_details_json) {
            try {
                details = JSON.parse(task.error_details_json);
            }
            catch {
                details = task.error_details_json;
            }
        }
        error = {
            code: task.error_code,
            message: task.error_message || '',
            details
        };
    }
    return {
        id: task.id,
        type: task.type,
        status: task.status,
        priority: task.priority,
        attempts: task.attempts,
        http_status: task.http_status ?? undefined,
        result,
        error,
        created_at: task.created_at,
        started_at: task.started_at,
        finished_at: task.finished_at
    };
}
function canonicalJson(obj) {
    if (obj === null || typeof obj !== 'object') {
        return JSON.stringify(obj);
    }
    if (Array.isArray(obj)) {
        return '[' + obj.map(canonicalJson).join(',') + ']';
    }
    const keys = Object.keys(obj).sort();
    return '{' + keys.map(k => JSON.stringify(k) + ':' + canonicalJson(obj[k])).join(',') + '}';
}
tasksRouter.post('/internal/v1/tasks', (req, res) => {
    const parsed = createTaskSchema.safeParse(req.body);
    if (!parsed.success) {
        res.status(400).json({
            code: 'VALIDATION_ERROR',
            message: 'Invalid task payload',
            details: parsed.error.format()
        });
        return;
    }
    const { type, priority, idempotency_key, actor, payload, resources } = parsed.data;
    const db = getSchedulerDb();
    // Check if idempotency_key already exists
    const existing = db.prepare('SELECT * FROM tasks WHERE idempotency_key = ?').get(idempotency_key);
    if (existing) {
        try {
            const existingPayload = JSON.parse(existing.payload_json);
            if (canonicalJson(existingPayload) !== canonicalJson(payload)) {
                res.status(422).json({
                    code: 'IDEMPOTENCY_KEY_REUSED',
                    message: 'Idempotency key reused with different request payload',
                    error: {
                        code: 'IDEMPOTENCY_KEY_REUSED',
                        message: 'Idempotency key reused with different request payload'
                    }
                });
                return;
            }
        }
        catch {
            // ignore
        }
        res.status(200).json(formatTaskDto(existing));
        return;
    }
    // De-duplicate and sort resources
    const sortedResources = Array.from(new Set(resources)).sort();
    const id = crypto.randomUUID();
    const nowIso = new Date().toISOString();
    const insertTx = db.transaction(() => {
        db.prepare(`
      INSERT INTO tasks (
        id, type, priority, idempotency_key, actor_json, payload_json, resources_json,
        status, attempts, max_attempts, created_at
      ) VALUES (?, ?, ?, ?, ?, ?, ?, 'QUEUED', 0, 3, ?)
    `).run(id, type, priority, idempotency_key, JSON.stringify(actor), JSON.stringify(payload), JSON.stringify(sortedResources), nowIso);
        db.prepare(`
      INSERT INTO task_events (task_id, at, event, details_json)
      VALUES (?, ?, 'ENQUEUED', NULL)
    `).run(id, nowIso);
    });
    insertTx();
    const scheduler = getScheduler(db);
    scheduler.enqueueImmediate();
    res.status(202).json({ id, status: 'QUEUED' });
});
tasksRouter.get('/internal/v1/tasks/:id', async (req, res) => {
    const { id } = req.params;
    const db = getSchedulerDb();
    const scheduler = getScheduler(db);
    const rawWait = req.query.wait_ms ? parseInt(req.query.wait_ms, 10) : 0;
    const waitMs = isNaN(rawWait) ? 0 : Math.min(Math.max(0, rawWait), 30000);
    let task = null;
    if (waitMs > 0) {
        task = await scheduler.waitForTask(id, waitMs);
    }
    else {
        task = db.prepare('SELECT * FROM tasks WHERE id = ?').get(id) || null;
    }
    if (!task) {
        res.status(404).json({
            code: 'TASK_NOT_FOUND',
            message: `Task ${id} not found`
        });
        return;
    }
    res.json(formatTaskDto(task));
});
tasksRouter.get('/internal/v1/tasks', (req, res) => {
    const db = getSchedulerDb();
    const status = req.query.status;
    const type = req.query.type;
    const page = Math.max(1, parseInt(req.query.page || '1', 10));
    const limit = Math.max(1, Math.min(100, parseInt(req.query.limit || '50', 10)));
    const offset = (page - 1) * limit;
    let query = 'SELECT * FROM tasks WHERE 1=1';
    const params = [];
    if (status) {
        query += ' AND status = ?';
        params.push(status);
    }
    if (type) {
        query += ' AND type = ?';
        params.push(type);
    }
    query += ' ORDER BY created_at DESC LIMIT ? OFFSET ?';
    params.push(limit, offset);
    const rows = db.prepare(query).all(...params);
    res.json({
        tasks: rows.map(formatTaskDto),
        page,
        limit
    });
});
tasksRouter.get('/internal/v1/locks', (_req, res) => {
    const db = getSchedulerDb();
    const rows = db.prepare(`
    SELECT l.resource, l.task_id, l.fencing_token, l.acquired_at, l.expires_at, t.type as task_type, t.priority as task_priority
    FROM locks l
    LEFT JOIN tasks t ON l.task_id = t.id
    ORDER BY l.acquired_at DESC
  `).all();
    res.json({ locks: rows });
});
tasksRouter.get('/internal/v1/queue', (_req, res) => {
    const db = getSchedulerDb();
    const now = new Date();
    const rows = db.prepare("SELECT * FROM tasks WHERE status = 'QUEUED' ORDER BY created_at ASC").all();
    const scored = rows.map((task) => ({
        ...formatTaskDto(task),
        effective_score: calculateEffectiveScore(task.priority, task.created_at, now),
        resources: JSON.parse(task.resources_json)
    }));
    scored.sort((a, b) => {
        if (b.effective_score !== a.effective_score)
            return b.effective_score - a.effective_score;
        if (a.created_at !== b.created_at)
            return a.created_at.localeCompare(b.created_at);
        return a.id.localeCompare(b.id);
    });
    res.json({ queue: scored });
});
tasksRouter.get('/internal/v1/stats', (_req, res) => {
    const db = getSchedulerDb();
    const queuedRow = db.prepare("SELECT COUNT(*) AS c FROM tasks WHERE status = 'QUEUED'").get();
    const runningRow = db.prepare("SELECT COUNT(*) AS c FROM tasks WHERE status = 'RUNNING'").get();
    const failedRow = db
        .prepare("SELECT COUNT(*) AS c FROM tasks WHERE status = 'FAILED' AND created_at >= datetime('now', '-24 hours')")
        .get();
    res.json({
        queued_tasks: queuedRow.c,
        running_tasks: runningRow.c,
        failed_tasks_24h: failedRow.c
    });
});
//# sourceMappingURL=tasks.js.map