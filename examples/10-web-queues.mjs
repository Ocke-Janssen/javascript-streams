const params = new URL(import.meta.url).searchParams;
const highWaterMark = Number(params.get('threshold') ?? globalThis.process?.argv[2] ?? 4);
const unit = params.get('unit') ?? globalThis.process?.argv[3] ?? 'bytes';
const fresh = params.get('fresh') === '1' || globalThis.process?.argv.includes('--fresh') === true;
if (!Number.isSafeInteger(highWaterMark) || highWaterMark < 0) {
  throw new RangeError('threshold must be a nonnegative integer');
}

if (unit !== 'bytes' && unit !== 'chunks') throw new RangeError('unit must be bytes or chunks');

function makeSource(traceQueue) {
  const strategy = unit === 'bytes'
    ? new ByteLengthQueuingStrategy({ highWaterMark })
    : new CountQueuingStrategy({ highWaterMark });
  return new ReadableStream({
    start(controller) {
      for (const chunk of [Uint8Array.of(1, 2), Uint8Array.of(3, 4, 5)]) {
        controller.enqueue(chunk);
        if (traceQueue) console.log(`enqueued=${chunk.byteLength} desiredSize=${controller.desiredSize}`);
      }
      controller.close();
    },
  }, strategy);
}
const source = makeSource(true);

const reader = source.getReader();
console.log(`locked=${source.locked}`);
try {
  try {
    source.getReader();
  } catch (error) {
    console.log(`second reader: ${error.name}`);
  }

  const { value } = await reader.read();
  console.log(`first=${value.join(',')}`);
} finally {
  reader.releaseLock();
}

console.log(`locked=${source.locked}`);
await source.pipeTo(new WritableStream({
  write(chunk) {
    console.log(`remaining=${chunk.join(',')}`);
  },
  close() {
    console.log('sink closed');
  },
}));
if (fresh) {
  const bytes = [];
  await makeSource(false).pipeTo(new WritableStream({
    write(chunk) {
      bytes.push(...chunk);
    },
  }));
  console.log(`fresh=${bytes.join(',')}`);
}
