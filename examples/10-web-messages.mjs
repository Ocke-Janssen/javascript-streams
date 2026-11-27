const trace = globalThis.process?.argv[2] === '--trace';
const bytes = new TextEncoder().encode(
  '{"id":1,"message":"Grüße"}\n' +
  '{"id":2,"message":"ready"}\n',
);
const cut = bytes.indexOf(0xc3) + 1;
const chunks = [bytes.subarray(0, cut), bytes.subarray(cut)];
if (trace) {
  console.log('split:', bytes[cut - 1].toString(16),
    '|', bytes[cut].toString(16));
}
let position = 0;

const source = new ReadableStream({
  pull(controller) {
    if (position === chunks.length) {
      controller.close();
      return;
    }
    controller.enqueue(chunks[position]);
    position += 1;
  },
});

let pending = '';
const maxLineUnits = 128;

function append(part) {
  if (pending.length + part.length > maxLineUnits) {
    throw new Error('line exceeds 128 UTF-16 code units');
  }
  pending += part;
}

const lines = new TransformStream({
  transform(text, controller) {
    if (trace) console.log('text:', JSON.stringify(text));
    let start = 0;
    let lf;
    while ((lf = text.indexOf('\n', start)) !== -1) {
      append(text.slice(start, lf));
      const line = pending.endsWith('\r') ? pending.slice(0, -1) : pending;
      if (line.includes('\r')) throw new Error('unexpected CR');
      if (trace) console.log('line:', line);
      controller.enqueue(JSON.parse(line));
      pending = '';
      start = lf + 1;
    }
    append(text.slice(start));
  },
  flush() {
    if (pending !== '') throw new Error('missing final LF');
  },
});

const display = new WritableStream({
  write(event) {
    if (!Number.isSafeInteger(event?.id) ||
        typeof event?.message !== 'string') {
      throw new Error('invalid message event');
    }
    console.log(event.id + ': ' + event.message);
  },
});

await source
  .pipeThrough(new TextDecoderStream('utf-8', { fatal: true }))
  .pipeThrough(lines)
  .pipeTo(display);
