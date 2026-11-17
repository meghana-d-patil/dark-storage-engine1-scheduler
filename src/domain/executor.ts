import Database from 'better-sqlite3';
import { TaskRecord } from '@dark-storage/shared';
import { config } from '../config.js';
import { extendLease, releaseLocks } from './lockManager.js';

const COMMAND_MAPPING: Record<string, string> = {
  PUTAWAY: 'putaway',
  RELOCATE: 'relocate',
  RESERVE_ORDER: 'reserve-order',
  PICK_CONFIRM: 'pick-confirm',
  EXCEPTION_RESOLVE: 'exception-resolve'
};

export type TaskExecutorFn = (task: TaskRecord, db: Database.Database) => Promise<{ status: number; body: any }>;

export async function defaultHttpExecutor(task: TaskRecord, db: Database.Database): Promise<{ status: number; body: any }> {
  const cmdName = COMMAND_MAPPING[task.type];
  if (!cmdName) {
    throw new Error(`Unknown task type command mapping: ${task.type}`);
  }

  // Get fencing tokens for resources
  const lockRows = db.prepare('SELECT resource, fencing_token FROM locks WHERE task_id = ?').all(task.id) as { resource: string; fencing_token: number }[];
  const tokenMap: Record<string, number> = {};
  for (const row of lockRows) {
    tokenMap[row.resource] = row.fencing_token;
  }

  const url = `${config.ENGINE3_URL}/internal/v1/commands/${cmdName}`;
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), config.ENGINE3_COMMAND_TIMEOUT_MS);

  try {
    const actor = JSON.parse(task.actor_json);
    const payload = JSON.parse(task.payload_json);

    const res = await fetch(url, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'x-internal-token': config.INTERNAL_TOKEN,
        Authorization: `Bearer ${config.INTERNAL_TOKEN}`,
        'X-Task-Id': task.id,
        'X-Lock-Tokens': JSON.stringify(tokenMap)
      },
      body: JSON.stringify({
        actor,
        idempotency_key: task.idempotency_key,
        payload
      }),
      signal: controller.signal
    });

    const body = await res.json().catch(() => ({}));
    return { status: res.status, body };
  } finally {
    clearTimeout(timer);
  }
}

export async function executeTask(
  db: Database.Database,
  task: TaskRecord,
  executor: TaskExecutorFn = defaultHttpExecutor
): Promise<void> {
  const nowIso = new Date().toISOString();

  // Get current attempts from DB
  const freshTask = db.prepare('SELECT attempts, max_attempts FROM tasks WHERE id = ?').get(task.id) as { attempts: number; max_attempts: number } | undefined;
  const currentAttempts = freshTask?.attempts ?? task.attempts;
  const maxAttempts = freshTask?.max_attempts ?? task.max_attempts;

  // Record EXECUTING event
  db.prepare(`
    INSERT INTO task_events (task_id, at, event, details_json)
    VALUES (?, ?, 'EXECUTING', NULL)
  `).run(task.id, nowIso);

  // Start lease heartbeat (every 10s extends by 30s)
  const heartbeat = setInterval(() => {
    try {
      extendLease(db, task.id, 30000);
    } catch {
      // ignore
    }
  }, 10000);

  try {
    let result: { status: number; body: any };
    try {
      result = await executor(task, db);
    } catch (err: any) {
      // Network error, fetch timeout, or thrown exception
      const isAbort = err.name === 'AbortError';
      const finishIso = new Date().toISOString();

      if (currentAttempts < maxAttempts) {
        const backoffMs = 1000 * Math.pow(2, currentAttempts - 1);
        const nextAttemptAt = new Date(Date.now() + backoffMs).toISOString();

        db.prepare(`
          UPDATE tasks
          SET status = 'QUEUED', next_attempt_at = ?
          WHERE id = ?
        `).run(nextAttemptAt, task.id);

        db.prepare(`
          INSERT INTO task_events (task_id, at, event, details_json)
          VALUES (?, ?, 'RETRY_SCHEDULED', ?)
        `).run(task.id, finishIso, JSON.stringify({
          error: isAbort ? 'Command timed out' : err.message,
          attempt: currentAttempts,
          next_attempt_at: nextAttemptAt
        }));
      } else {
        db.prepare(`
          UPDATE tasks
          SET status = 'FAILED', error_code = 'ENGINE3_UNAVAILABLE', error_message = ?, finished_at = ?
          WHERE id = ?
        `).run(isAbort ? 'Command timed out' : (err.message || 'Engine 3 unavailable'), finishIso, task.id);

        db.prepare(`
          INSERT INTO task_events (task_id, at, event, details_json)
          VALUES (?, ?, 'FAILED', ?)
        `).run(task.id, finishIso, JSON.stringify({
          error_code: 'ENGINE3_UNAVAILABLE',
          error_message: isAbort ? 'Command timed out' : err.message
        }));
      }
      return;
    }

    const finishIso = new Date().toISOString();

    if (result.status >= 200 && result.status < 300) {
      // SUCCEEDED
      db.prepare(`
        UPDATE tasks
        SET status = 'SUCCEEDED', http_status = ?, result_json = ?, finished_at = ?
        WHERE id = ?
      `).run(result.status, JSON.stringify(result.body), finishIso, task.id);

      db.prepare(`
        INSERT INTO task_events (task_id, at, event, details_json)
        VALUES (?, ?, 'SUCCEEDED', NULL)
      `).run(task.id, finishIso);
    } else if (result.status >= 400 && result.status < 500) {
      // 4xx domain error: FAILED immediately, NO retry
      const errCode = result.body?.error?.code || result.body?.code || 'DOMAIN_ERROR';
      const errMsg = result.body?.error?.message || result.body?.message || 'Domain error';
      const rawDetails = result.body?.error?.details || result.body?.details;
      const errDetails = rawDetails ? JSON.stringify(rawDetails) : null;

      db.prepare(`
        UPDATE tasks
        SET status = 'FAILED', http_status = ?, error_code = ?, error_message = ?, error_details_json = ?, finished_at = ?
        WHERE id = ?
      `).run(result.status, errCode, errMsg, errDetails, finishIso, task.id);

      db.prepare(`
        INSERT INTO task_events (task_id, at, event, details_json)
        VALUES (?, ?, 'FAILED', ?)
      `).run(task.id, finishIso, JSON.stringify({
        error_code: errCode,
        error_message: errMsg
      }));
    } else {
      // 5xx error from Engine 3
      if (currentAttempts < maxAttempts) {
        const backoffMs = 1000 * Math.pow(2, currentAttempts - 1);
        const nextAttemptAt = new Date(Date.now() + backoffMs).toISOString();

        db.prepare(`
          UPDATE tasks
          SET status = 'QUEUED', next_attempt_at = ?
          WHERE id = ?
        `).run(nextAttemptAt, task.id);

        db.prepare(`
          INSERT INTO task_events (task_id, at, event, details_json)
          VALUES (?, ?, 'RETRY_SCHEDULED', ?)
        `).run(task.id, finishIso, JSON.stringify({
          status: result.status,
          error: result.body?.message || 'Server error',
          attempt: currentAttempts,
          next_attempt_at: nextAttemptAt
        }));
      } else {
        db.prepare(`
          UPDATE tasks
          SET status = 'FAILED', error_code = 'ENGINE3_UNAVAILABLE', error_message = ?, finished_at = ?
          WHERE id = ?
        `).run(result.body?.message || `Engine 3 returned ${result.status}`, finishIso, task.id);

        db.prepare(`
          INSERT INTO task_events (task_id, at, event, details_json)
          VALUES (?, ?, 'FAILED', ?)
        `).run(task.id, finishIso, JSON.stringify({
          error_code: 'ENGINE3_UNAVAILABLE',
          status: result.status
        }));
      }
    }
  } finally {
    clearInterval(heartbeat);
    releaseLocks(db, task.id);
  }
}

