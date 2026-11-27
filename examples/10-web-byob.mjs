const input = Uint8Array.of(65, 66, 67);
let offset = 0;

const source = new ReadableStream({
  type: 'bytes',
  pull(controller) {
    const request = controller.byobRequest;
    if (!request) throw new Error('this example expects a BYOB reader');
    if (offset === input.length) {
      controller.close();
      request.respond(0);
      return;
    }
    const count = Math.min(request.view.byteLength, input.length - offset);
    request.view.set(input.subarray(offset, offset + count));
    offset += count;
    request.respond(count);
  },
});

const reader = source.getReader({ mode: 'byob' });
try {
  const firstBuffer = new Uint8Array(2);
  const first = await reader.read(firstBuffer);
  if (first.done || first.value.join(',') !== '65,66' || firstBuffer.buffer.byteLength !== 0) {
    throw new Error('unexpected first read');
  }
  console.log(`firstInputDetached=${firstBuffer.buffer.byteLength === 0} first=${first.value.join(',')}`);

  const secondBuffer = new Uint8Array(first.value.buffer);
  const second = await reader.read(secondBuffer);
  if (second.done || second.value.join(',') !== '67' || first.value.buffer.byteLength !== 0) {
    throw new Error('unexpected second read');
  }
  console.log(`firstResultDetached=${first.value.buffer.byteLength === 0} second=${second.value.join(',')}`);

  const end = await reader.read(new Uint8Array(2));
  if (!end.done) throw new Error('expected end of stream');
  console.log(`done=${end.done}`);
} finally {
  reader.releaseLock();
}
