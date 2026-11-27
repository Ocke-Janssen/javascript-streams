import { createReadStream } from 'node:fs';
import { mkdir, mkdtemp } from 'node:fs/promises';
import { join } from 'node:path';
import { buildOutput, limits, newCounters, PipelineError } from './pipeline.mjs';

const [fixture = 'demo', lineLimit] = process.argv.slice(2);
const lineBytes = lineLimit === undefined ? limits.lineBytes : Number(lineLimit);
if (!['demo', 'invalid'].includes(fixture) || process.argv.length > 4 ||
    !Number.isSafeInteger(lineBytes) || lineBytes < 1 || lineBytes > limits.lineBytes) {
  console.error('Usage: node projects/event-pipeline/run-file.mjs [demo|invalid] [max-line-bytes]');
  process.exitCode = 1;
} else {
  const controller = new AbortController();
  const stop = () => controller.abort();
  const counters = newCounters();
  process.once('SIGINT', stop);
  try {
    const root = join('scratch', 'event-pipeline');
    await mkdir(root, { recursive: true });
    const directory = await mkdtemp(join(root, 'local-'));
    const output = join(directory, 'result.ndjson.gz');
    const input = new URL('fixtures/' + fixture + '.ndjson', import.meta.url);
    const result = await buildOutput({
      openSource: () => createReadStream(input, { highWaterMark: 16 * 1024 }),
      output,
      signal: controller.signal,
      policy: { ...limits, lineBytes },
      counters,
    });
    console.log(JSON.stringify({ status: 'ok', output, ...result }));
  } catch (error) {
    const aborted = error.name === 'AbortError';
    const reason = aborted ? 'cancelled' : error instanceof PipelineError ? error.code : 'io_error';
    console.error(JSON.stringify({
      status: aborted ? 'aborted' : 'error', reason, line: error.line,
      cleanupPending: counters.cleanupPending ?? false,
    }));
    process.exitCode = 1;
  } finally {
    process.off('SIGINT', stop);
  }
}
