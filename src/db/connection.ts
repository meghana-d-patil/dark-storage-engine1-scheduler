import Database from 'better-sqlite3';
import { config } from '../config.js';

let schedulerDbInstance: Database.Database | null = null;

export function openSchedulerDb(dbPath: string = config.SCHEDULER_DB_PATH): Database.Database {
  const db = new Database(dbPath);
  db.pragma('journal_mode = WAL');
  db.pragma('synchronous = NORMAL');
  db.pragma('foreign_keys = ON');
  db.pragma('busy_timeout = 5000');
  return db;
}

export function getSchedulerDb(): Database.Database {
  if (!schedulerDbInstance) {
    schedulerDbInstance = openSchedulerDb();
  }
  return schedulerDbInstance;
}

export function closeSchedulerDb(): void {
  if (schedulerDbInstance) {
    schedulerDbInstance.close();
    schedulerDbInstance = null;
  }
}
