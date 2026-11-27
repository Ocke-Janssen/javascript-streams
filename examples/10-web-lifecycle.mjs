const params = new URL(import.meta.url).searchParams;
const replacement = params.get('replacement') === '1' ||
  globalThis.process?.argv.includes('--replacement') === true;
const keepSink = params.get('keepSink') === '1' ||
  globalThis.process?.argv.includes('--keep-sink') === true;

async function transformAndClose() {
  const source = new ReadableStream({
    start(controller) {
      controller.enqueue('alpha');
      controller.enqueue('beta');
      controller.close();
    },
  });

  let count = 0;
  const transform = new TransformStream({
    transform(word, controller) {
      count += 1;
      controller.enqueue(word.toUpperCase());
    },
    flush(controller) {
      controller.enqueue(`count=${count}`);
    },
  });

  await source.pipeThrough(transform).pipeTo(new WritableStream({
    write(line) {
      console.log(line);
    },
    close() {
      console.log('normal: sink closed');
    },
  }));
}

async function stopPipe(mode) {
  const operation = new AbortController();
  const cause = new Error(`${mode} requested`);
  const source = new ReadableStream({
    pull(controller) {
      controller.enqueue('sample');
    },
    cancel(reason) {
      console.log(`${mode}: source cancelled (${reason.message})`);
    },
  }, { highWaterMark: 0 });

  const sink = new WritableStream({
    write() {
      if (mode === 'abort') {
        operation.abort(cause);
      } else {
        throw cause;
      }
    },
    abort(reason) {
      console.log(`${mode}: sink aborted (${reason.message})`);
    },
  });

  try {
    await source.pipeTo(sink, { signal: operation.signal });
  } catch (error) {
    if (error !== cause) throw error;
    console.log(`${mode}: pipe rejected`);
  }
}

async function decodeInvalidInput() {
  const source = new ReadableStream({
    start(controller) {
      controller.enqueue(Uint8Array.of(0xc3));
      controller.close();
    },
  });

  try {
    await source
      .pipeThrough(new TextDecoderStream('utf-8', { fatal: !replacement }))
      .pipeTo(new WritableStream({
        write(text) {
          console.log(`replacement=${JSON.stringify(text)}`);
        },
        abort(reason) {
          console.log(`invalid UTF-8: sink aborted (${reason.name})`);
        },
      }));
  } catch (error) {
    if (!(error instanceof TypeError)) throw error;
    console.log(`invalid UTF-8: ${error.name}`);
  }
}

async function finishManually() {
  const source = new ReadableStream({
    start(controller) {
      controller.enqueue('payload');
      controller.close();
    },
  });
  const sink = new WritableStream({
    write(value) {
      console.log('write=' + value);
    },
    close() {
      console.log('sink closed');
    },
  });
  await source.pipeTo(sink, { preventClose: true });
  console.log(`pipe fulfilled sourceLocked=${source.locked} sinkLocked=${sink.locked}`);
  const writer = sink.getWriter();
  try {
    await writer.write('footer');
    await writer.close();
  } finally {
    writer.releaseLock();
  }
}

if (keepSink) {
  await finishManually();
} else {
  await transformAndClose();
  await stopPipe('abort');
  await stopPipe('failure');
  await decodeInvalidInput();
}
