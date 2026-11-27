import { once } from 'node:events';
import { PassThrough } from 'node:stream';
import { finished } from 'node:stream/promises';

const [mode = 'drain'] = process.argv.slice(2);
if (!['drain', 'close'].includes(mode)) {
  console.error('Usage: node examples/04-drain-close.mjs [drain|close]');
  process.exit(1);
}

const stage = new PassThrough({ highWaterMark: 1 });
console.log(`write=${stage.write('x')} pendingBytes=${stage.writableLength}`);

const stopWait = new AbortController();
let wait = 'pending';
const watchDrain = once(stage, 'drain', { signal: stopWait.signal }).then(
  () => { wait = 'drain'; },
  (error) => { wait = error.name; },
);
const watchCompletion = finished(stage, { readable: false, cleanup: true }).then(
  () => 'finish',
  (error) => error.code,
);

if (mode === 'drain') {
  stage.resume();
  await watchDrain;
  stage.end();
  console.log(`wait=${wait} completion=${await watchCompletion}`);
} else {
  stage.destroy();
  console.log(`completion=${await watchCompletion} waitAtClose=${wait}`);
  stopWait.abort();
  await watchDrain;
  console.log(`waitAfterAbort=${wait} drainListeners=${stage.listenerCount('drain')}`);
}
