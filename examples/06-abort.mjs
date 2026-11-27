import { mkdtemp, open, rename, rm } from 'node:fs/promises';
import { dirname, join, resolve } from 'node:path';
import { Writable } from 'node:stream';
import { pipeline } from 'node:stream/promises';
import { setTimeout as delay } from 'node:timers/promises';

const [mode, outputPath] = process.argv.slice(2);
if (!['ok', 'error', 'abort', 'timeout'].includes(mode) || !outputPath) {
  console.error('Usage: node examples/06-abort.mjs <ok|error|abort|timeout> <output>');
  process.exit(1);
}

const controller = new AbortController();
const output = resolve(outputPath);

async function* records({ signal }) {
  try {
    yield 'first\n';
    if (mode === 'abort' || mode === 'timeout') {
      // A cancellable stand-in for waiting on an external source.
      await delay(60_000, undefined, { signal });
    }
    yield 'second\n';
  } finally {
    console.log('source cleanup');
  }
}

function requestStopAfterFirstWrite() {
  if (mode === 'abort') {
    controller.abort(new Error('manual request'));
  } else if (mode === 'timeout') {
    const deadline = AbortSignal.timeout(25);
    deadline.addEventListener('abort', () => controller.abort(deadline.reason), { once: true });
  }
}

async function writeOutput() {
  const directory = await mkdtemp(join(dirname(output), '.streams-abort-'));
  const partial = join(directory, 'output.txt');
  try {
    const file = await open(partial, 'wx');
    let pipelineSucceeded = false;
    try {
      const sink = new Writable({
        objectMode: true,
        write(record, _encoding, done) {
          if (mode === 'error' && record === 'second\n') {
            done(new Error('write rejected'));
            return;
          }

          file.writeFile(record).then(() => {
            console.log(`written=${record.trimEnd()}`);
            done();
            if (record === 'first\n') requestStopAfterFirstWrite();
          }, done);
        },
      });
      await pipeline(records, sink, { signal: controller.signal });
      pipelineSucceeded = true;
    } finally {
      // FileHandle.close() waits for pending operations on this handle.
      try {
        await file.close();
        console.log('file closed');
      } catch (error) {
        if (pipelineSucceeded) throw error;
        console.error(`close warning: ${error.code ?? error.message}`);
      }
    }
    await rename(partial, output);
    console.log('published');
  } finally {
    try {
      await rm(directory, { recursive: true, force: true });
      console.log('temporary removed');
    } catch (error) {
      console.error(`cleanup warning for ${directory}: ${error.code ?? error.message}`);
    }
  }
}

try {
  await writeOutput();
} catch (error) {
  if (controller.signal.aborted && error.code === 'ABORT_ERR') {
    console.error(`aborted=${mode} reason=${controller.signal.reason.name}`);
  } else {
    console.error(`failed: ${error.message}`);
  }
  process.exitCode = 1;
}
