import { Readable } from 'node:stream';
import { finished } from 'node:stream/promises';

const mode = process.argv[2] ?? 'iterate';
const source = Readable.from([Buffer.from('ab'), Buffer.from('cd'), Buffer.from('ef')], {
  objectMode: false,
});

let text = '';
function consume(chunk) {
  text += chunk.toString('ascii');
  console.log(`received=${chunk.toString('ascii')} flowing=${source.readableFlowing}`);
}

switch (mode) {
  case 'data': {
    const done = finished(source, { cleanup: true });
    source.on('pause', () => console.log('source pause'));
    source.on('resume', () => console.log('source resume'));
    source.on('data', (chunk) => {
      consume(chunk);
      if (text.length === 2) {
        source.pause();
        console.log(`paused flowing=${source.readableFlowing}`);
        setImmediate(() => source.resume());
      }
    });
    await done;
    break;
  }
  case 'readable': {
    const done = finished(source, { cleanup: true });
    source.on('readable', () => {
      let chunk;
      while ((chunk = source.read()) !== null) {
        consume(chunk);
      }
    });
    await done;
    break;
  }
  case 'iterate':
    for await (const chunk of source) {
      consume(chunk);
    }
    break;
  default:
    console.error('Usage: node examples/02-read-modes.mjs [data|readable|iterate]');
    process.exit(1);
}

console.log(`result=${text}`);
