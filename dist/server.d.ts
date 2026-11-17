import { Express } from 'express';
import type Database from 'better-sqlite3';
import { Scheduler } from './domain/scheduler.js';
export declare function createApp(): Express;
export declare function startSchedulerService(db?: Database.Database): Scheduler;
//# sourceMappingURL=server.d.ts.map