import { setTimeout as delay } from 'node:timers/promises';

const mode = process.argv[2] ?? 'tee';
const total = Number(process.argv[3] ?? 16);
if (!['tee', 'paired', 'paired-fail'].includes(mode) || !Number.isInteger(total) ||
    total < (mode === 'paired-fail' ? 2 : 1) || total > 64) {
  console.error('Usage: node examples/12-tee.mjs [tee|paired|paired-fail] [blocks: 1–64, at least 2 for paired-fail]');
  process.exit(1);
}
const blockBytes = 1024;
let produced = 0;
let fastCount = 0;
let slowCount = 0;
let peak = 0;
const pairedFailure = new Error('slow sink rejected block 2');
const secondFastWrite = Promise.withResolvers();

const source = new ReadableStream({
  pull(controller) {
    if (produced === total) {
      controller.close();
      return;
    }
    produced += 1;
    peak = Math.max(peak, (produced - slowCount) * blockBytes);
    controller.enqueue(new Uint8Array(blockBytes));
  },
}, { highWaterMark: 0 });

const fastSink = new WritableStream({
  write() {
    fastCount += 1;
  },
});
const slowSink = new WritableStream({
  async write() {
    if (mode === 'paired-fail' && slowCount === 1) {
      await secondFastWrite.promise;
      throw pairedFailure;
    }
    await delay(10);
    slowCount += 1;
  },
});

if (mode === 'tee') {
  const [fast, slow] = source.tee();
  try {
    await fast.pipeTo(fastSink, { preventCancel: true });
  } catch (error) {
    await Promise.allSettled([fast.cancel(error), slow.cancel(error)]);
    throw error;
  }
  console.log(`before slow starts: fast=${fastCount} slow=${slowCount}`);
  await slow.pipeTo(slowSink);
} else {
  const fast = fastSink.getWriter();
  const slow = slowSink.getWriter();
  try {
    for await (const chunk of source) {
      const gated = mode === 'paired-fail' && slowCount === 1;
      const fastWrite = fast.write(chunk).then(() => {
        if (gated) secondFastWrite.resolve();
      }, (error) => {
        if (gated) secondFastWrite.reject(error);
        throw error;
      });
      const results = await Promise.allSettled([fastWrite, slow.write(chunk)]);
      const failure = results.find((result) => result.status === 'rejected');
      if (failure) throw failure.reason;
    }
    await Promise.all([fast.close(), slow.close()]);
  } catch (error) {
    await Promise.allSettled([fast.abort(error), slow.abort(error)]);
    if (error !== pairedFailure) throw error;
    console.error('result=failed: ' + error.message);
    process.exitCode = 1;
  } finally {
    fast.releaseLock();
    slow.releaseLock();
  }
}

console.log(`mode=${mode} fast=${fastCount} slow=${slowCount} peakUnfinishedSlowBytes=${peak}`);
