import { createReadStream, createWriteStream } from 'node:fs';
import { mkdtemp, realpath, rename, rm, stat } from 'node:fs/promises';
import { dirname, join, resolve } from 'node:path';
import { pipeline } from 'node:stream/promises';
import { createGzip } from 'node:zlib';

export async function compressFile(inputPath, output, { beforeRename } = {}) {
  const inputInfo = await stat(inputPath, { bigint: true });
  const outputInfo = await stat(output, { bigint: true }).catch((error) => {
    if (error.code !== 'ENOENT') throw error;
    return null;
  });
  if (outputInfo && inputInfo.dev === outputInfo.dev && inputInfo.ino === outputInfo.ino) {
    throw new Error('input and output refer to the same file');
  }

  const directory = await mkdtemp(join(dirname(output), '.streams-gzip-'));
  const partial = join(directory, 'output.gz');
  try {
    await pipeline(
      createReadStream(inputPath),
      createGzip(),
      createWriteStream(partial, { flags: 'wx' }),
    );
    if (beforeRename) await beforeRename(partial);
    await rename(partial, output);
  } finally {
    await rm(directory, { recursive: true, force: true }).catch((error) => {
      console.error(`cleanup warning for ${directory}: ${error.code ?? error.message}`);
    });
  }
}

const entry = process.argv[1]
  ? await realpath(process.argv[1]).catch(() => undefined)
  : undefined;
if (entry && entry === await realpath(import.meta.filename)) {
  const [inputPath, outputPath] = process.argv.slice(2);
  if (!inputPath || !outputPath) {
    console.error('Usage: node examples/03-gzip-file.mjs <input> <output.gz>');
    process.exitCode = 1;
  } else {
    try {
      await compressFile(inputPath, resolve(outputPath));
      console.log('gzip ready');
    } catch (error) {
      console.error(`compression failed: ${error.code ?? error.message}`);
      if (error.syscall === 'rename') console.error('operation=rename');
      process.exitCode = 1;
    }
  }
}
