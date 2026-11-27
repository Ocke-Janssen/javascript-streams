import { readFile } from 'node:fs/promises';
import { createServer } from 'node:http';
import { pipeline } from 'node:stream/promises';
import { setTimeout as delay } from 'node:timers/promises';

const port = Number(process.env.PORT ?? 3003);
const assets = new Map();
for (const [path, file, type] of [
  ['/', 'index.html', 'text/html; charset=utf-8'],
  ['/app.mjs', 'app.mjs', 'text/javascript; charset=utf-8'],
  ['/records.mjs', 'records.mjs', 'text/javascript; charset=utf-8'],
  ['/upload.mjs', 'upload.mjs', 'text/javascript; charset=utf-8'],
]) {
  assets.set(path, {
    type,
    content: await readFile(new URL(file, import.meta.url)),
  });
}

const messages = ['Grüße', 'Reading', '<b>literal text</b>', 'Processing', 'Saving', 'Done'];
const uploadSample = Buffer.from([0, 65, 255, 10, 0]);

function json(response, status, body, close = false) {
  const headers = { 'Content-Type': 'application/json; charset=utf-8' };
  if (close) headers.Connection = 'close';
  response.writeHead(status, headers).end(JSON.stringify(body));
}

async function inspectUpload(request, response) {
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), 2000);
  const abort = () => request.destroy();
  controller.signal.addEventListener('abort', abort, { once: true });

  const chunks = [];
  let count = 0;
  try {
    for await (const chunk of request.iterator({ destroyOnReturn: false })) {
      count += chunk.length;
      if (count > 64) {
        json(response, 413, { error: 'probe body exceeds 64 bytes' }, true);
        return;
      }
      chunks.push(chunk);
    }
    const body = Buffer.concat(chunks, count);
    json(response, 200, { bytes: count, match: body.equals(uploadSample) });
  } finally {
    clearTimeout(timeout);
    controller.signal.removeEventListener('abort', abort);
  }
}

async function sendFeed(response, variant) {
  const controller = new AbortController();
  const disconnected = () => {
    if (!response.writableFinished) controller.abort();
  };
  response.once('close', disconnected);

  response.setHeader('Content-Type', 'application/x-ndjson; charset=utf-8');
  response.setHeader('Cache-Control', 'no-cache, no-transform');

  try {
    await pipeline(async function* feed({ signal }) {
      try {
        const input = variant === 'fast'
          ? Array.from({ length: 24 }, (_, index) => `Record ${index + 1}`)
          : messages;
        const recordCount = variant === 'short' ? 2 : input.length;
        for (let index = 0; index < recordCount; index += 1) {
          const message = input[index];
          if (index > 0 && variant !== 'fast') await delay(300, undefined, { signal });
          if (variant === 'truncated' && index === 2) {
            yield '{"id":3,"message":';
            return;
          }

          const id = variant === 'gap' && index >= 2 ? index + 2 : index + 1;
          const line = Buffer.from(JSON.stringify({ id, message }) + '\n');
          if (index === 0 && variant !== 'fast') {
            const split = line.indexOf(0xc3) + 1;
            yield line.subarray(0, split);
            await delay(45, undefined, { signal });
            yield line.subarray(split);
          } else {
            yield line;
          }
        }
      } finally {
        console.log(`feed released case=${variant}`);
      }
    }, response, { signal: controller.signal });
  } catch (error) {
    console.log('feed stopped:', error.code ?? error.name);
  } finally {
    response.off('close', disconnected);
  }
}

async function route(request, response) {
  const url = new URL(request.url, 'http://127.0.0.1');

  if (request.method === 'POST' && url.pathname === '/api/upload') {
    await inspectUpload(request, response);
    return;
  }
  if (request.method !== 'GET') {
    json(response, 405, { error: 'method not allowed' });
    return;
  }

  const asset = assets.get(url.pathname);
  if (asset) {
    response.writeHead(200, { 'Content-Type': asset.type }).end(asset.content);
    return;
  }
  if (url.pathname !== '/api/feed') {
    json(response, 404, { error: 'not found' });
    return;
  }

  const variant = url.searchParams.get('case') ?? 'ok';
  if (variant === 'http-error') {
    json(response, 503, { error: 'demo unavailable' });
  } else if (variant === 'no-body') {
    response.writeHead(204).end();
  } else if (['ok', 'truncated', 'fast', 'short', 'gap'].includes(variant)) {
    await sendFeed(response, variant);
  } else {
    json(response, 400, { error: 'unknown scenario' });
  }
}

const server = createServer((request, response) => {
  route(request, response).catch((error) => {
    console.error('request failed:', error.code ?? error.name);
    if (!response.headersSent && !response.destroyed) {
      json(response, 500, { error: 'request failed' }, true);
    } else {
      response.destroy();
    }
  });
});
server.listen(port, '127.0.0.1', () => {
  console.log('Listening on http://127.0.0.1:' + server.address().port);
});
