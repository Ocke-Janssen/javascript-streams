import { createReadStream } from 'node:fs';
import { PassThrough, Readable } from 'node:stream';
import { pipeline } from 'node:stream/promises';

const [mode = 'web', path = 'fixtures/events.ndjson'] = process.argv.slice(2);
if (!['node', 'web'].includes(mode)) {
  console.error('Usage: node examples/12-interop.mjs [node|web] [input]');
  process.exit(1);
}

const threshold = 1024;
const input = createReadStream(path, { highWaterMark: threshold });

if (mode === 'node') {
  await pipeline(input, new PassThrough(), process.stdout);
} else {
  const webInput = Readable.toWeb(input, {
    strategy: { highWaterMark: threshold, size: (chunk) => chunk.byteLength },
  });
  const identity = new TransformStream({
    transform(chunk, controller) {
      controller.enqueue(chunk);
    },
  });
  const nodeOutput = Readable.fromWeb(webInput.pipeThrough(identity), {
    highWaterMark: threshold,
  });
  await pipeline(nodeOutput, process.stdout);
}
