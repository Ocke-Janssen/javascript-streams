import { Duplex, Readable, Transform, Writable } from 'node:stream';
import { pipeline } from 'node:stream/promises';

const direction = process.argv[2] ?? 'from';
const words = ['alpha', 'beta'];
function sink() {
  return new Writable({
    objectMode: true,
    write(word, _encoding, done) {
      console.log(word);
      done();
    },
  });
}

if (direction === 'from') {
  const web = new TransformStream({
    transform(word, controller) {
      controller.enqueue(word.toUpperCase());
    },
  });
  await pipeline(Readable.from(words), Duplex.fromWeb(web, { objectMode: true }), sink());
} else if (direction === 'to') {
  const node = new Transform({
    objectMode: true,
    transform(word, _encoding, done) {
      done(null, word.toUpperCase());
    },
  });
  await Readable.toWeb(Readable.from(words))
    .pipeThrough(Duplex.toWeb(node))
    .pipeTo(Writable.toWeb(sink()));
} else {
  console.error('Usage: node examples/12-duplex.mjs [from|to]');
  process.exitCode = 1;
}
