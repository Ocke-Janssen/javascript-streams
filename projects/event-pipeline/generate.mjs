import { pipeline } from 'node:stream/promises';

const count = Number(process.argv[2] ?? '12');
if (!Number.isSafeInteger(count) || count < 1 || count > 100_000) {
  console.error('Usage: node projects/event-pipeline/generate.mjs [records: 1–100000]');
  process.exitCode = 1;
} else {
  await pipeline(async function* generate() {
    const levels = ['info', 'debug', 'warn'];
    for (let id = 1; id <= count; id += 1) {
      const level = levels[(id - 1) % levels.length];
      yield JSON.stringify({ id, level, event: 'Grüße ' + id }) + '\n';
    }
  }, process.stdout);
}
