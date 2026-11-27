import { Writable } from 'node:stream';
import { finished } from 'node:stream/promises';

for (const decodeStrings of [true, false]) {
  let releaseWrite;
  const sink = new Writable({
    highWaterMark: 3,
    decodeStrings,
    write(_chunk, _encoding, done) {
      releaseWrite = done;
    },
  });
  const completed = finished(sink, { cleanup: true });
  const ready = sink.write('🧪');
  console.log(`decodeStrings=${decodeStrings} pending=${sink.writableLength} ready=${ready}`);
  releaseWrite();
  sink.end();
  await completed;
}
