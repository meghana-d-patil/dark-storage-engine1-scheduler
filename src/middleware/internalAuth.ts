import { Request, Response, NextFunction } from 'express';
import { config } from '../config.js';

export function internalAuth(req: Request, res: Response, next: NextFunction): void {
  const authHeader = req.headers.authorization;
  const bearerToken = authHeader?.startsWith('Bearer ') ? authHeader.slice(7).trim() : null;
  const token = req.headers['x-internal-token'] || bearerToken;
  const requestId = (req.headers['x-request-id'] as string) || crypto.randomUUID();

  if (!token || token !== config.INTERNAL_TOKEN) {
    res.status(401).json({
      error: {
        code: 'UNAUTHENTICATED',
        message: 'Invalid or missing internal token',
        request_id: requestId
      }
    });
    return;
  }

  next();
}
