import { Request, Response, NextFunction } from 'express';

export function createAuthMiddleware() {
  const configuredKey = process.env.AETHERSCHED_API_KEY;

  return (req: Request, res: Response, next: NextFunction) => {
    // If no key is configured in env, auth is disabled for local dev ease
    if (!configuredKey) {
      return next();
    }

    // Public exempt routes
    const exemptPaths = [
      '/health',
      '/metrics',
      '/api/v1/telemetry',
      '/api/v1/telemetry/stream',
    ];

    if (exemptPaths.some((p) => req.path.startsWith(p)) || !req.path.startsWith('/api/')) {
      return next();
    }

    const apiKeyHeader = req.header('X-API-Key');
    const authHeader = req.header('Authorization');
    const bearerToken = authHeader?.startsWith('Bearer ') ? authHeader.substring(7) : null;

    const providedKey = apiKeyHeader || bearerToken;

    if (!providedKey || providedKey !== configuredKey) {
      return res.status(401).json({
        success: false,
        error: 'Unauthorized: Invalid or missing X-API-Key / Authorization header.',
      });
    }

    next();
  };
}
