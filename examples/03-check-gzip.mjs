import { createReadStream } from 'node:fs';
import { open } from 'node:fs/promises';
import { pipeline } from 'node:stream/promises';
import { createGunzip } from 'node:zlib';

async function compareGzip(inputPath, compressedPath) {
  const original = await open(inputPath, 'r');
  let bytes = 0;

  try {
    await pipeline(createReadStream(compressedPath), createGunzip(), async (decoded) => {
      for await (const actual of decoded) {
        const expected = Buffer.alloc(actual.length);
        let offset = 0;
        while (offset < expected.length) {
          const { bytesRead } = await original.read(expected, offset, expected.length - offset);
          if (bytesRead === 0) throw new Error('decoded file is longer');
          offset += bytesRead;
        }

        if (!expected.equals(actual)) throw new Error(`different bytes at or after offset ${bytes}`);
        bytes += actual.length;
      }
    });

    const { bytesRead } = await original.read(Buffer.alloc(1), 0, 1);
    if (bytesRead !== 0) throw new Error('decoded file is shorter');
    return bytes;
  } finally {
    await original.close();
  }
}

const [inputPath, compressedPath] = process.argv.slice(2);
if (!inputPath || !compressedPath) {
  console.error('Usage: node examples/03-check-gzip.mjs <input> <output.gz>');
  process.exitCode = 1;
} else {
  try {
    console.log(`sameBytes=${await compareGzip(inputPath, compressedPath)}`);
  } catch (error) {
    console.error(`check failed: ${error.code ?? error.message}`);
    process.exitCode = 1;
  }
}
