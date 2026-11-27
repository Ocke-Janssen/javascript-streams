import { Writable } from 'node:stream';
import { pipeline } from 'node:stream/promises';

async function* bounded(source, limit) {
  let seen = 0;
  for await (const chunk of source) {
    seen += chunk.byteLength;
    if (seen > limit) {
      throw new Error('input_limit');
    }
    yield chunk;
  }
}

export async function copy(chunks, limit) {
  const parts = [];
  const sink = new Writable({
    write(chunk, _encoding, done) {
      parts.push(chunk);
      done();
    },
  });
  await pipeline(
    chunks,
    source => bounded(source, limit),
    sink,
  );
  return Buffer.concat(parts);
}
