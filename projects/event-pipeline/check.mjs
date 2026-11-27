import { createHash } from 'node:crypto';
import { createReadStream } from 'node:fs';
import { Writable } from 'node:stream';
import { pipeline } from 'node:stream/promises';
import { createGunzip } from 'node:zlib';
import { limitBytes } from './pipeline.mjs';

const [input, expectation] = process.argv.slice(2);
const demo = expectation === '--demo';
const expected = demo
  ? 'fc127eb395903d16763493bfc297ccdf57f00bd91dde9c5c41876261ec5e26ef'
  : expectation;
if (!input || (expected !== undefined && !/^[a-f0-9]{64}$/.test(expected))) {
  console.error('Usage: node projects/event-pipeline/check.mjs <file.ndjson.gz> [sha256|--demo]');
  process.exitCode = 1;
} else {
  const digest = createHash('sha256');
  let bytes = 0;
  let records = 0;
  const sample = [];
  try {
    await pipeline(
      createReadStream(input),
      (source) => limitBytes(source, 16 * 1024 * 1024, 'compressed_limit'),
      createGunzip(),
      (source) => limitBytes(source, 16 * 1024 * 1024, 'expanded_limit'),
      new Writable({
        write(chunk, _encoding, done) {
          digest.update(chunk);
          bytes += chunk.length;
          if (demo && bytes <= 356) sample.push(chunk);
          for (const byte of chunk) {
            if (byte === 0x0a) records += 1;
          }
          done();
        },
      }),
    );
    const outputSha256 = digest.digest('hex');
    const match = expected === undefined ? null : expected === outputSha256;
    const result = { bytes, records, outputSha256, match };
    if (demo && match && bytes === 356) {
      result.ids = Buffer.concat(sample).toString('utf8').trimEnd().split('\n')
        .map((line) => JSON.parse(line).id);
    }
    console.log(JSON.stringify(result));
    if (match === false) process.exitCode = 1;
  } catch (error) {
    const reason = ['compressed_limit', 'expanded_limit'].includes(error.code)
      ? error.code : 'invalid_gzip_or_io_error';
    console.error(JSON.stringify({ reason }));
    process.exitCode = 1;
  }
}
