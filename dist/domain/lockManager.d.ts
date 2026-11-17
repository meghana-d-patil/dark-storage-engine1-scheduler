import Database from 'better-sqlite3';
export interface TaskToLock {
    id: string;
    resources_json?: string;
    resources?: string[];
}
export declare function tryAcquireAll(db: Database.Database, task: TaskToLock, now?: Date): boolean;
export declare function releaseLocks(db: Database.Database, taskId: string, event?: string): void;
export declare function extendLease(db: Database.Database, taskId: string, extendMs?: number): void;
export declare function sweepExpiredLocks(db: Database.Database, now?: Date, runningTaskIds?: Set<string>): number;
//# sourceMappingURL=lockManager.d.ts.map