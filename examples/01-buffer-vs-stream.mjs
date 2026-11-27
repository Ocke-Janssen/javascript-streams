import { createReadStream } from 'node:fs';
import { readFile } from 'node:fs/promises';

const [mode, inputPath, chunkArg = '64'] = process.argv.slice(2);
const chunkBytes = Number(chunkArg);

if (!inputPath || !['buffer', 'stream'].includes(mode) ||
    !Number.isInteger(chunkBytes) || chunkBytes < 1 || chunkBytes > 1_048_576) {
  console.error('Usage: node examples/01-buffer-vs-stream.mjs <buffer|stream> <input> [chunk-bytes: 1–1048576]');
  process.exit(1);
}

const started = performance.now();
let bytes = 0;
let chunks = 0;
let maxChunkBytes = 0;

function count(chunk) {
  bytes += chunk.length;
  chunks += 1;
  maxChunkBytes = Math.max(maxChunkBytes, chunk.length);

  if (chunks === 1) {
    const firstMs = (performance.now() - started).toFixed(2);
    console.log(`firstChunkBytes=${chunk.length} firstMs=${firstMs}`);
  }
}

if (mode === 'buffer') {
  const input = await readFile(inputPath);
  if (input.length > 0) count(input);
} else {
  const source = createReadStream(inputPath, {
    highWaterMark: chunkBytes,
  });
  for await (const chunk of source) {
    count(chunk);
  }
}

if (chunks === 0) console.log('firstChunkBytes=none firstMs=none');
console.log(`mode=${mode} bytes=${bytes} chunks=${chunks} maxChunkBytes=${maxChunkBytes}`);
const totalMs = (performance.now() - started).toFixed(2);
console.log(`totalMs=${totalMs} processPeakRssKiB=${process.resourceUsage().maxRSS}`);
