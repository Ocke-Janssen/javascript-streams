import { PassThrough, Readable, Writable } from 'node:stream';
import { pipeline } from 'node:stream/promises';

const mode = process.argv[2] ?? 'ok';
if (!['ok', 'source', 'sink'].includes(mode)) {
  console.error('Usage: node examples/03-pipeline-errors.mjs [ok|source|sink]');
  process.exit(1);
}

function* chunks() {
  yield Buffer.from('alpha');
  if (mode === 'source') throw new Error('source failed');
  yield Buffer.from('beta');
}

const source = Readable.from(chunks(), { objectMode: false });
const stage = new PassThrough();
const sink = new Writable({
  write(chunk, _encoding, done) {
    const text = chunk.toString('ascii');
    if (mode === 'sink' && text === 'beta') {
      done(new Error('sink failed'));
      return;
    }

    console.log(`accepted=${text}`);
    done();
  },
});

try {
  await pipeline(source, stage, sink);
  console.log('pipeline complete');
} catch (error) {
  console.error(`pipeline failed: ${error.message}`);
  process.exitCode = 1;
} finally {
  console.log(`closed source=${source.closed} stage=${stage.closed} sink=${sink.closed}`);
}
