import Database from 'better-sqlite3';
import { TaskRecord } from '@dark-storage/shared';
import { TaskExecutorFn } from './executor.js';
export interface SchedulerOptions {
    maxConcurrency?: number;
    tickIntervalMs?: number;
    sweepIntervalMs?: number;
    executor?: TaskExecutorFn;
}
export declare class Scheduler {
    private db;
    private maxConcurrency;
    private tickIntervalMs;
    private sweepIntervalMs;
    private executor;
    private runningTasks;
    private tickTimer;
    private sweepTimer;
    private emitter;
    private stopped;
    constructor(db: Database.Database, options?: SchedulerOptions);
    setExecutor(executor: TaskExecutorFn): void;
    start(): void;
    stop(): void;
    recoverCrash(): void;
    sweep(now?: Date): number;
    tick(now?: Date): void;
    enqueueImmediate(): void;
    waitForTask(taskId: string, timeoutMs?: number): Promise<TaskRecord | null>;
}
export declare function getScheduler(db?: Database.Database): Scheduler;
export declare function setSchedulerInstance(scheduler: Scheduler | null): void;
//# sourceMappingURL=scheduler.d.ts.map