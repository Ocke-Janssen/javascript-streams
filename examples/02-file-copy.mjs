import { createReadStream, createWriteStream } from 'node:fs';
import { mkdtemp, readFile, rm, stat } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { pipeline } from 'node:stream/promises';

const verify = process.argv[2] === '--verify';
const fixture = 'fixtures/events.ndjson';
const inputPath = verify ? fixture : process.argv[2] ?? fixture;
const directory = await mkdtemp(join(tmpdir(), 'streams-copy-'));
const copyPath = join(directory, 'copy.ndjson');

try {
  await pipeline(createReadStream(inputPath), createWriteStream(copyPath));
  console.log(`copyBytes=${(await stat(copyPath)).size}`);
  if (verify) {
    const sameBytes = (await readFile(inputPath)).equals(await readFile(copyPath));
    console.log(`sameBytes=${sameBytes}`);
    if (!sameBytes) process.exitCode = 1;
  }
} finally {
  await rm(directory, { recursive: true, force: true });
}
