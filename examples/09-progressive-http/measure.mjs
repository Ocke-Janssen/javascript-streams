import { parseDemoSse, readDemoFrames } from './frames.mjs';

const [kind = 'ndjson', option] = process.argv.slice(2);
const reconnect = kind === 'sse' && ['--reconnect', '--replay-one', '--replay-conflict'].includes(option);
const noFinal = kind === 'sse' && option === '--no-final';
if (!['json', 'ndjson', 'sse'].includes(kind) || (option && !reconnect && !noFinal)) {
  console.error('Usage: node examples/09-progressive-http/measure.mjs [json|ndjson|sse] [--reconnect|--replay-one|--replay-conflict|--no-final]');
  process.exit(1);
}

const port = Number(process.env.PORT ?? 3002);
const base = `http://127.0.0.1:${port}`;
const started = performance.now();
const progressCount = 4;
const seen = new Map();
let lastId = 0;
let sawEnd = false;

function mark(name) {
  console.log(`${name} elapsedMs=${Math.round(performance.now() - started)}`);
}

function accept(item) {
  if (!Number.isInteger(item?.id) || item.id < 1 || item.id > progressCount) {
    throw new Error('unexpected progress ID');
  }
  if (seen.has(item.id)) {
    if (seen.get(item.id) !== item.percent) throw new Error(`conflicting replay id=${item.id}`);
    mark(`duplicate ignored id=${item.id}`);
    return;
  }
  if (item.id !== lastId + 1 || item.percent !== item.id * 25) {
    throw new Error('unexpected progression');
  }
  seen.set(item.id, item.percent);
  lastId = item.id;
  mark(`event id=${item.id} percent=${item.percent}`);
}

function acceptDemoSse(frame) {
  const event = parseDemoSse(frame);
  if (!event) return;
  if (event.type === 'end') {
    const endId = progressCount + 1;
    if (sawEnd || event.id !== String(endId) || event.data !== '{}' ||
        seen.size !== progressCount) {
      throw new Error('unexpected SSE end event');
    }
    sawEnd = true;
    mark(`end id=${endId}`);
    return;
  }
  if (event.type !== 'progress') return;
  if (sawEnd) throw new Error('progress after SSE end event');
  const item = JSON.parse(event.data);
  if (String(item?.id) !== event.id) throw new Error('inconsistent SSE progress ID');
  accept(item);
}

async function readPath(path, headers = {}) {
  const response = await fetch(base + path, {
    headers,
    signal: AbortSignal.timeout(5000),
  });
  if (!response.ok || !response.body) throw new Error(`status=${response.status}`);
  mark(`status=${response.status} type=${response.headers.get('content-type')}`);
  if (kind === 'json') {
    for (const item of await response.json()) accept(item);
  } else if (kind === 'ndjson') {
    await readDemoFrames(response.body, '\n', (line) => accept(JSON.parse(line)));
  } else {
    await readDemoFrames(response.body, '\n\n', acceptDemoSse);
  }
  mark('response ended');
}

const path = '/' + kind;
await readPath(path + (reconnect ? '?interrupt=1' : noFinal ? '?without-end=1' : ''));
if (reconnect) {
  if (lastId !== 2 || sawEnd) throw new Error('expected interruption after progress ID 2');
  const cursor = option === '--replay-one' ? lastId - 1 : lastId;
  mark(`resuming after id=${cursor}`);
  const query = option === '--replay-conflict' ? '?replay-conflict=1' : '';
  await readPath(path + query, { 'Last-Event-ID': String(cursor) });
}
if (seen.size !== progressCount) throw new Error(`expected ${progressCount} progress events, received ${seen.size}`);
if (kind === 'sse' && !sawEnd) throw new Error('missing SSE end event');
