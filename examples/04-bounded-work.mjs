import { Readable } from 'node:stream';
import { setTimeout as delay } from 'node:timers/promises';

const limit = 2;
const fail = process.argv.includes('--fail');
let active = 0;
let peak = 0;

async function processRecord(id) {
  active += 1;
  peak = Math.max(peak, active);
  try {
    if (fail && id === 2) throw new Error('record 2 failed');
    await delay(10);
    return id;
  } finally {
    active -= 1;
  }
}

async function processBatch(batch) {
  const results = await Promise.allSettled(batch.map(processRecord));
  const failure = results.find((result) => result.status === 'rejected');
  if (failure) throw failure.reason;
  console.log(`completed=${results.map((result) => result.value).join(',')}`);
}

try {
  let batch = [];
  for await (const id of Readable.from([1, 2, 3, 4, 5])) {
    batch.push(id);
    if (batch.length === limit) {
      await processBatch(batch);
      batch = [];
    }
  }
  if (batch.length > 0) await processBatch(batch);
} catch (error) {
  console.error(error.message);
  process.exitCode = 1;
} finally {
  console.log(`peakActive=${peak} active=${active}`);
}
