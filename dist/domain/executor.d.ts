import Database from 'better-sqlite3';
import { TaskRecord } from '@dark-storage/shared';
export type TaskExecutorFn = (task: TaskRecord, db: Database.Database) => Promise<{
    status: number;
    body: any;
}>;
export declare function defaultHttpExecutor(task: TaskRecord, db: Database.Database): Promise<{
    status: number;
    body: any;
}>;
export declare function executeTask(db: Database.Database, task: TaskRecord, executor?: TaskExecutorFn): Promise<void>;
//# sourceMappingURL=executor.d.ts.map