import { once } from 'node:events';
import { PassThrough } from 'node:stream';
import { finished } from 'node:stream/promises';

function listenerCount(stream) {
  return ['end', 'finish', 'error', 'close']
    .reduce((count, event) => count + stream.listenerCount(event), 0);
}

const normal = new PassThrough();
const before = listenerCount(normal);
const complete = finished(normal, { cleanup: true });
normal.resume();
normal.end('sample');
await complete;
console.log(`success closed=${normal.closed} listenerDelta=${listenerCount(normal) - before}`);

const premature = new PassThrough();
const abandoned = finished(premature, { cleanup: true });
premature.destroy();
try {
  await abandoned;
} catch (error) {
  if (error.code !== 'ERR_STREAM_PREMATURE_CLOSE') throw error;
  console.log(`premature=${error.code}`);
}

const observed = new PassThrough();
const controller = new AbortController();
const observing = finished(observed, { signal: controller.signal, cleanup: true });
controller.abort();

try {
  await observing;
} catch (error) {
  if (error.name !== 'AbortError') throw error;
  console.log(`waitAborted=${error.name} streamDestroyed=${observed.destroyed}`);
} finally {
  const closed = once(observed, 'close');
  observed.destroy();
  await closed;
  console.log(`ownerClosed=${observed.closed}`);
}
