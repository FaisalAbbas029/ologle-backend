import { describe, expect, it } from 'vitest';
import handler from '../api/index.ts';
import { EventEmitter } from 'events';

// Lightweight mock for Node http.IncomingMessage and http.ServerResponse
function createMockHttp(options: {
  method?: string;
  url?: string;
  headers?: Record<string, string>;
}) {
  const req: any = new EventEmitter();
  req.method = options.method || 'GET';
  req.url = options.url || '/';
  req.headers = { ...(options.headers || {}) };
  req.socket = new EventEmitter();

  const res: any = new EventEmitter();
  res.headers = {} as Record<string, string>;
  res.statusCode = 200;
  res.body = '';

  res.setHeader = (key: string, value: string) => {
    res.headers[key.toLowerCase()] = value;
    return res;
  };
  res.getHeader = (key: string) => res.headers[key.toLowerCase()];
  res.status = (code: number) => {
    res.statusCode = code;
    return res;
  };
  res.writeHead = (code: number, headers?: Record<string, string>) => {
    res.statusCode = code;
    if (headers) {
      for (const [k, v] of Object.entries(headers)) {
        res.setHeader(k, v);
      }
    }
    return res;
  };
  res.json = (data: any) => {
    res.body = JSON.stringify(data);
    res.emit('finish');
    return res;
  };
  res.end = (chunk?: any) => {
    if (chunk) res.body += chunk.toString();
    res.emit('finish');
    return res;
  };

  return { req, res };
}

describe('Vercel API Entrypoint (api/index.ts)', () => {
  it('handles GET /health and returns healthy status with redis status telemetry', async () => {
    const { req, res } = createMockHttp({
      method: 'GET',
      url: '/health',
      headers: {
        origin: 'https://ologle-five.vercel.app',
      },
    });

    await handler(req, res);

    expect(res.statusCode).toBe(200);
    const parsed = JSON.parse(res.body);
    expect(parsed.status).toBe('ok');
    expect(parsed.service).toBe('ologle-signaling');
    expect(typeof parsed.activeSessions).toBe('number');
    expect(typeof parsed.queuedUsers).toBe('number');
    expect(parsed.redis).toBeDefined();
    expect(typeof parsed.redis.connected).toBe('boolean');
    expect(res.headers['access-control-allow-origin']).toBe('https://ologle-five.vercel.app');
  });

  it('handles GET /api/health identically through rewrite route', async () => {
    const { req, res } = createMockHttp({
      method: 'GET',
      url: '/api/health',
      headers: {
        origin: 'https://ologle-five.vercel.app',
      },
    });

    await handler(req, res);

    expect(res.statusCode).toBe(200);
    const parsed = JSON.parse(res.body);
    expect(parsed.status).toBe('ok');
    expect(res.headers['access-control-allow-origin']).toBe('https://ologle-five.vercel.app');
  });

  it('handles GET / root metadata endpoint', async () => {
    const { req, res } = createMockHttp({
      method: 'GET',
      url: '/',
    });

    await handler(req, res);

    expect(res.statusCode).toBe(200);
    const parsed = JSON.parse(res.body);
    expect(parsed.service).toBe('ologle-signaling');
    expect(parsed.endpoints.websocket).toBe('/ws');
  });

  it('rejects CORS requests from unauthorized origins', async () => {
    const { req, res } = createMockHttp({
      method: 'GET',
      url: '/api/ice-servers',
      headers: {
        origin: 'https://malicious-phishing-site.com',
      },
    });

    await handler(req, res);

    expect(res.statusCode).toBe(403);
    const parsed = JSON.parse(res.body);
    expect(parsed.title).toBe('Forbidden');
  });

  it('allows CORS requests from production frontend https://ologle-five.vercel.app', async () => {
    const { req, res } = createMockHttp({
      method: 'GET',
      url: '/api/ice-servers',
      headers: {
        origin: 'https://ologle-five.vercel.app',
      },
    });

    await handler(req, res);

    expect(res.statusCode).toBe(200);
    expect(res.headers['access-control-allow-origin']).toBe('https://ologle-five.vercel.app');
  });

  it('handles OPTIONS preflight requests for https://ologle-five.vercel.app with 204 No Content', async () => {
    const { req, res } = createMockHttp({
      method: 'OPTIONS',
      url: '/api/ice-servers',
      headers: {
        origin: 'https://ologle-five.vercel.app',
      },
    });

    await handler(req, res);

    expect(res.statusCode).toBe(204);
    expect(res.headers['access-control-allow-origin']).toBe('https://ologle-five.vercel.app');
    expect(res.headers['access-control-allow-methods']).toContain('GET');
  });

  it('rejects HTTP GET /ws without WebSocket upgrade header with 426 Upgrade Required', async () => {
    const { req, res } = createMockHttp({
      method: 'GET',
      url: '/ws',
    });

    await handler(req, res);

    expect(res.statusCode).toBe(426);
    expect(res.headers['upgrade']).toBe('WebSocket');
    expect(res.body).toContain('Upgrade Required');
  });

  it('rejects WebSocket upgrade request from unauthorized origin with 403', async () => {
    const { req, res } = createMockHttp({
      method: 'GET',
      url: '/ws',
      headers: {
        upgrade: 'websocket',
        origin: 'https://attacker-domain.evil',
      },
    });

    await handler(req, res);

    expect(res.statusCode).toBe(403);
    expect(res.body).toContain('Origin not allowed');
  });
});
