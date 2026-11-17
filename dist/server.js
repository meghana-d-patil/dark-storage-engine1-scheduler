import express from 'express';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { config } from './config.js';
import { healthRouter } from './routes/health.js';
import { internalAuth } from './middleware/internalAuth.js';
import { tasksRouter } from './routes/tasks.js';
import { getSchedulerDb, closeSchedulerDb } from './db/connection.js';
import { getScheduler } from './domain/scheduler.js';
export function createApp() {
    const app = express();
    app.use(express.json({ limit: '1mb' }));
    app.use(healthRouter);
    app.use(internalAuth);
    app.use(tasksRouter);
    return app;
}
let activeScheduler = null;
export function startSchedulerService(db = getSchedulerDb()) {
    const scheduler = getScheduler(db);
    scheduler.start();
    activeScheduler = scheduler;
    return scheduler;
}
const isMain = process.argv[1] && (path.resolve(process.argv[1]) === fileURLToPath(import.meta.url) ||
    path.resolve(process.argv[1]).replace(/\\/g, '/').endsWith('services/engine1-scheduler/src/server.ts') ||
    path.resolve(process.argv[1]).replace(/\\/g, '/').endsWith('src/server.ts'));
if (isMain) {
    const db = getSchedulerDb();
    startSchedulerService(db);
    const app = createApp();
    const server = app.listen(config.PORT, config.HOST, () => {
        console.log(`Engine 1 (Scheduler) listening on http://${config.HOST}:${config.PORT}`);
    });
    const cleanup = () => {
        if (activeScheduler) {
            activeScheduler.stop();
        }
        closeSchedulerDb();
        server.close(() => process.exit(0));
    };
    process.on('SIGTERM', cleanup);
    process.on('SIGINT', cleanup);
}
//# sourceMappingURL=server.js.map