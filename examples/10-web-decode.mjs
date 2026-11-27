const bytes = new TextEncoder().encode('Grüße\n');
const params = new URL(import.meta.url).searchParams;
const split = Number(params.get('split') ?? globalThis.process?.argv[2] ?? 3);
if (!Number.isInteger(split) || split < 1 || split >= bytes.length) {
  throw new RangeError('split must be inside the UTF-8 byte sequence');
}
const chunks = [bytes.subarray(0, split), bytes.subarray(split)];
let index = 0;

const source = new ReadableStream({
  pull(controller) {
    if (index === chunks.length) {
      controller.close();
      return;
    }

    controller.enqueue(chunks[index]);
    index += 1;
  },
});

await source
  .pipeThrough(new TextDecoderStream('utf-8', { fatal: true }))
  .pipeTo(new WritableStream({
    write(text) {
      console.log(JSON.stringify(text));
    },
  }));
