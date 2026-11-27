import { once } from 'node:events';
import { Writable } from 'node:stream';
import { finished } from 'node:stream/promises';

const threshold = Number(process.argv[2] ?? 4);
const terminalMode = process.argv[2];
const showLength = process.argv[3] === '--length';

async function observeRejectedWrite(mode) {
  let processed = 0;
  const probe = new Writable({
    write(_chunk, _encoding, done) {
      processed += 1;
      done();
    },
  });
  probe.on('error', () => {});
  if (mode === '--after-end') probe.end();
  else probe.destroy();

  const { promise: failure, resolve } = Promise.withResolvers();
  const returned = probe.write('late', (error) => resolve(error?.code));
  console.log(`write late ${returned}`);
  console.log(`callback=${await failure} processed=${processed}`);
}

if (['--after-end', '--after-destroy'].includes(terminalMode)) {
  await observeRejectedWrite(terminalMode);
} else if (!Number.isSafeInteger(threshold) || threshold < 0 ||
    (process.argv[3] !== undefined && !showLength) || process.argv.length > 4) {
  console.error('Usage: node examples/04-backpressure.mjs [nonnegative threshold in bytes [--length] | --after-end | --after-destroy]');
  process.exit(1);
} else {
  await observeBackpressure(threshold, showLength);
}

async function observeBackpressure(threshold, showLength) {
  const sink = new Writable({
    highWaterMark: threshold,
    write(chunk, encoding, callback) {
      setTimeout(() => {
        console.log('processed', chunk.toString());
        callback();
      }, 10);
    },
  });

  for (const chunk of ['aa', 'bb', 'cc']) {
    const canContinue = sink.write(chunk);
    console.log('write', chunk, canContinue);
    if (showLength) console.log(`writableLength=${sink.writableLength}`);
    if (!canContinue) {
      await once(sink, 'drain');
      console.log('drain');
    }
  }

  sink.end();
  await finished(sink, { cleanup: true });
}
