import { Router } from 'express';

export const healthRouter = Router();

healthRouter.get('/health', (req, res) => {
  res.json({ status: 'ok', service: 'engine1-scheduler' });
});
