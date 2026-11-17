import Database from 'better-sqlite3';
import { config } from '../config.js';
let schedulerDbInstance = null;
export function openSchedulerDb(dbPath = config.SCHEDULER_DB_PATH) {
    const db = new Database(dbPath);
    db.pragma('journal_mode = WAL');
    db.pragma('synchronous = NORMAL');
    db.pragma('foreign_keys = ON');
    db.pragma('busy_timeout = 5000');
    return db;
}
export function getSchedulerDb() {
    if (!schedulerDbInstance) {
        schedulerDbInstance = openSchedulerDb();
    }
    return schedulerDbInstance;
}
export function closeSchedulerDb() {
    if (schedulerDbInstance) {
        schedulerDbInstance.close();
        schedulerDbInstance = null;
    }
}
//# sourceMappingURL=connection.js.map