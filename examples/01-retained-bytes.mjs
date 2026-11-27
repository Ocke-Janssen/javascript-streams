import { createReadStream } from 'node:fs';

const [mode, input = 'fixtures/events.ndjson'] = process.argv.slice(2);
if (mode !== 'count' && mode !== 'retain') {
  console.error('Usage: node examples/01-retained-bytes.mjs <count|retain> [input]');
  process.exitCode = 1;
} else {
  const maxBytes = 1024 * 1024;
  let bytes = 0;
  const kept = [];
  for await (const chunk of createReadStream(input)) {
    bytes += chunk.length;
    if (bytes > maxBytes) throw new RangeError('Use at most 1 MiB of input for this probe');
    if (mode === 'retain') kept.push(chunk);
  }
  const retained = kept.reduce((total, chunk) => total + chunk.length, 0);
  console.log(`bytes=${bytes} retained=${retained}`);
}
