import { readFile } from 'node:fs/promises';
import { createServer } from 'node:http';
import { pipeline } from 'node:stream/promises';
import { setTimeout as delay } from 'node:timers/promises';

const port = Number(process.env.PORT ?? 3002);
const page = await readFile(new URL('./index.html', import.meta.url));
const events = [25, 50, 75, 100].map((percent, index) => ({ id: index + 1, percent }));
const endId = events.at(-1).id + 1;
const interval = 240;
const types = {
  json: 'application/json; charset=utf-8',
  ndjson: 'application/x-ndjson; charset=utf-8',
  sse: 'text/event-stream; charset=utf-8',
};

function sseFrame(item) {
  return `id: ${item.id}\nevent: progress\ndata: ${JSON.stringify(item)}\n\n`;
}

async function* produce(kind, cursor, { available, interrupt, withoutEnd, replayConflict, signal }) {
  if (kind === 'json') {
    await delay(interval * (events.length - 1), undefined, { signal });
    yield JSON.stringify(events) + '\n';
    return;
  }

  if (kind === 'sse') yield 'retry: 250\n\n';
  if (replayConflict) yield sseFrame({ ...events[1], percent: 51 });
  const remaining = available.filter(({ id }) => id > cursor);
  for (const [index, item] of remaining.entries()) {
    if (index > 0) {
      await delay(interval / 2, undefined, { signal });
      if (kind === 'sse') yield ': heartbeat\n\n';
      await delay(interval / 2, undefined, { signal });
    }
    yield kind === 'sse' ? sseFrame(item) : JSON.stringify(item) + '\n';
    if (interrupt && item.id === 2) return;
  }
  if (kind === 'sse' && !withoutEnd) yield `id: ${endId}\nevent: end\ndata: {}\n\n`;
}

function sendReplayState(response, status, state) {
  response.writeHead(status, {
    'Content-Type': types.json,
    'Cache-Control': 'no-store',
  }).end(JSON.stringify(state) + '\n');
}

async function route(request, response) {
  if (request.method !== 'GET') {
    response.writeHead(405, { Allow: 'GET' }).end();
    return;
  }
  const url = new URL(request.url, 'http://127.0.0.1');
  if (url.pathname === '/') {
    response.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8' }).end(page);
    return;
  }
  if (url.pathname === '/snapshot') {
    const { percent, id: throughId } = events.at(-1);
    sendReplayState(response, 200, { percent, throughId });
    return;
  }

  const kind = url.pathname.slice(1);
  if (!Object.hasOwn(types, kind)) {
    response.writeHead(404).end();
    return;
  }

  const bounded = kind === 'sse' && url.searchParams.get('window') === 'bounded';
  const available = bounded ? events.slice(-2) : events;
  let cursor = 0;
  if (kind === 'sse') {
    const header = request.headers['last-event-id'];
    cursor = Number(header ?? 0);
    if (header !== undefined && (String(cursor) !== header || !Number.isInteger(cursor) ||
        cursor < 0 || cursor > endId)) {
      response.writeHead(400).end('Unknown event ID\n');
      return;
    }
    console.log(`sse cursor=${cursor}`);
    if (cursor === endId) {
      response.writeHead(204).end();
      return;
    }
    const earliestId = available[0].id;
    if (cursor + 1 < earliestId) {
      sendReplayState(response, 409, { reason: 'replay_gap', earliestId });
      return;
    }
  }
  const interrupt = kind === 'sse' && cursor === 0 && url.searchParams.get('interrupt') === '1';
  const withoutEnd = kind === 'sse' && url.searchParams.get('without-end') === '1';
  const replayConflict = kind === 'sse' && !bounded && cursor === 2 &&
    url.searchParams.get('replay-conflict') === '1';
  const controller = new AbortController();
  function disconnected() {
    if (!response.writableFinished) controller.abort();
  }
  response.once('close', disconnected);
  response.setHeader('Content-Type', types[kind]);
  response.setHeader('Cache-Control', 'no-cache, no-transform');

  try {
    await pipeline(({ signal }) => produce(kind, cursor, { available, interrupt, withoutEnd, replayConflict, signal }),
      response, { signal: controller.signal });
  } catch (error) {
    console.error('delivery stopped:', error.code ?? error.name);
  } finally {
    response.off('close', disconnected);
  }
}

const server = createServer((request, response) => {
  route(request, response).catch((error) => {
    console.error('request failed:', error.code ?? error.name);
    if (!response.headersSent && !response.destroyed) {
      response.writeHead(500).end();
    } else {
      response.destroy();
    }
  });
});
server.listen(port, '127.0.0.1', () => {
  console.log('Listening on http://127.0.0.1:' + server.address().port);
});
