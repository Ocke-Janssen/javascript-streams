import { execFileSync } from 'node:child_process';
import { mkdtempSync, readFileSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { fileURLToPath } from 'node:url';

const interop = fileURLToPath(new URL('./12-interop.mjs', import.meta.url));
const ndjson = fileURLToPath(new URL('../fixtures/events.ndjson', import.meta.url));
const binary = Buffer.from([0, 65, 255, 10, 0]);
const temporary = mkdtempSync(join(tmpdir(), 'streams-interop-'));

try {
  const binaryFile = join(temporary, 'sample.bin');
  writeFileSync(binaryFile, binary);

  for (const [label, path, expected] of [
    ['ndjson', ndjson, readFileSync(ndjson)],
    ['binary', binaryFile, binary],
  ]) {
    const outputs = ['node', 'web'].map((mode) =>
      execFileSync(process.execPath, [interop, mode, path], { maxBuffer: 64 * 1024 }));
    const same = outputs.every((output) => expected.equals(output));
    console.log(`${label} sameBytes=${same}`);
    if (!same) process.exitCode = 1;
  }
} finally {
  rmSync(temporary, { recursive: true, force: true });
}
