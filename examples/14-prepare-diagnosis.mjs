import { spawnSync } from 'node:child_process';
import { randomInt } from 'node:crypto';
import { mkdir, mkdtemp, rm, writeFile } from 'node:fs/promises';
import { join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { summarizeRunLog } from './14-run-log.mjs';

const scenarios = ['slow-source', 'slow-sink', 'bad-record'];
for (let index = scenarios.length - 1; index > 0; index -= 1) {
  const other = randomInt(index + 1);
  [scenarios[index], scenarios[other]] = [scenarios[other], scenarios[index]];
}

const observer = fileURLToPath(new URL('./14-observe-pipeline.mjs', import.meta.url));
const cases = [];
const runIds = new Set();
for (const [index, mode] of scenarios.entries()) {
  const result = spawnSync(process.execPath, [observer, mode, '--blind']);
  if (result.error) throw result.error;

  const expectedFailure = mode === 'bad-record';
  if (result.status !== (expectedFailure ? 1 : 0)) {
    throw new Error(`${mode}: unexpected child process outcome`);
  }
  const end = summarizeRunLog(result.stdout, mode);
  if (end.status !== (expectedFailure ? 'error' : 'ok') ||
      end.reason !== (expectedFailure ? 'invalid_json' : 'completed') ||
      end.mode !== undefined || runIds.has(end.runId)) {
    throw new Error(`${mode}: unexpected run outcome or reused ID`);
  }
  runIds.add(end.runId);
  cases.push({ path: `case-${index + 1}.ndjson`, mode, output: result.stdout });
}

await mkdir('scratch', { recursive: true });
const directory = await mkdtemp(join('scratch', '14-diagnosis-'));
try {
  const key = {};
  for (const { path, mode, output } of cases) {
    await writeFile(join(directory, path), output, { flag: 'wx' });
    key[path] = mode;
  }
  await writeFile(join(directory, 'answer-key.json'), JSON.stringify(key, null, 2) + '\n', { flag: 'wx' });
} catch (error) {
  await rm(directory, { recursive: true, force: true });
  throw error;
}
console.log(directory);
