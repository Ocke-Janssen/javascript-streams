let finishWrite;
const written = new Promise((resolve) => {
  finishWrite = resolve;
});

const sink = new WritableStream({
  write(chunk) {
    console.log(`handling=${chunk}`);
    return written;
  },
}, new CountQueuingStrategy({ highWaterMark: 2 }));

const writer = sink.getWriter();
try {
  const firstWrite = writer.write('alpha');
  await writer.ready;
  console.log(`ready desiredSize=${writer.desiredSize}`);

  finishWrite();
  await firstWrite;
  console.log('write complete');

  await writer.close();
  console.log('closed');
} finally {
  writer.releaseLock();
}
