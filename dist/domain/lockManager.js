export function tryAcquireAll(db, task, now = new Date()) {
    const nowIso = now.toISOString();
    const expiresAt = new Date(now.getTime() + 30000).toISOString();
    const resources = typeof task.resources_json === 'string'
        ? JSON.parse(task.resources_json)
        : (task.resources || []);
    const acquireTx = db.transaction(() => {
        // 1. Delete expired locks
        db.prepare('DELETE FROM locks WHERE expires_at <= ?').run(nowIso);
        // 2. Check if any resource is locked
        if (resources.length > 0) {
            const placeholders = resources.map(() => '?').join(',');
            const locked = db.prepare(`SELECT resource FROM locks WHERE resource IN (${placeholders})`).all(...resources);
            if (locked.length > 0) {
                return false;
            }
        }
        // 3. All free, acquire locks
        for (const res of resources) {
            db.prepare('UPDATE lock_tokens SET next_token = next_token + 1 WHERE id = 1').run();
            const tokenRow = db.prepare('SELECT next_token - 1 AS token FROM lock_tokens WHERE id = 1').get();
            const fencingToken = tokenRow.token;
            db.prepare(`
        INSERT INTO locks (resource, task_id, fencing_token, acquired_at, expires_at)
        VALUES (?, ?, ?, ?, ?)
      `).run(res, task.id, fencingToken, nowIso, expiresAt);
        }
        // 4. Update task status
        db.prepare(`
      UPDATE tasks
      SET status = 'RUNNING', attempts = attempts + 1, started_at = ?
      WHERE id = ?
    `).run(nowIso, task.id);
        // 5. Emit event
        db.prepare(`
      INSERT INTO task_events (task_id, at, event, details_json)
      VALUES (?, ?, 'LOCKS_ACQUIRED', NULL)
    `).run(task.id, nowIso);
        return true;
    });
    return acquireTx();
}
export function releaseLocks(db, taskId, event = 'LOCKS_RELEASED') {
    const nowIso = new Date().toISOString();
    const releaseTx = db.transaction(() => {
        db.prepare('DELETE FROM locks WHERE task_id = ?').run(taskId);
        db.prepare('INSERT INTO task_events (task_id, at, event, details_json) VALUES (?, ?, ?, NULL)').run(taskId, nowIso, event);
    });
    releaseTx();
}
export function extendLease(db, taskId, extendMs = 30000) {
    const expiresAt = new Date(Date.now() + extendMs).toISOString();
    db.prepare('UPDATE locks SET expires_at = ? WHERE task_id = ?').run(expiresAt, taskId);
}
export function sweepExpiredLocks(db, now = new Date(), runningTaskIds = new Set()) {
    const nowIso = now.toISOString();
    const expired = db.prepare('SELECT resource, task_id FROM locks WHERE expires_at <= ?').all(nowIso);
    if (expired.length === 0)
        return 0;
    const toRemove = expired.filter((l) => !runningTaskIds.has(l.task_id));
    if (toRemove.length === 0)
        return 0;
    const tx = db.transaction(() => {
        for (const item of toRemove) {
            db.prepare('INSERT INTO task_events (task_id, at, event, details_json) VALUES (?, ?, ?, ?)').run(item.task_id, nowIso, 'LOCK_EXPIRED', JSON.stringify({ resource: item.resource }));
            db.prepare('DELETE FROM locks WHERE resource = ?').run(item.resource);
        }
    });
    tx();
    return toRemove.length;
}
//# sourceMappingURL=lockManager.js.map