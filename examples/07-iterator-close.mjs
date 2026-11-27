import { Readable } from 'node:stream';

async function* records() {
  try {
    yield 'first';
    yield 'second';
  } finally {
    console.log('generator cleanup');
  }
}

for await (const record of records()) {
  console.log(`generator=${record}`);
  break;
}

const ordinary = Readable.from(['first', 'second']);
const closed = new Promise((resolve) => ordinary.once('close', resolve));
for await (const record of ordinary) {
  console.log(`default=${record}`);
  break;
}
await closed;
console.log(`default destroyed=${ordinary.destroyed}`);

const reusable = Readable.from(['first', 'second']);
for await (const record of reusable.iterator({ destroyOnReturn: false })) {
  console.log(`kept=${record}`);
  break;
}
console.log(`kept destroyed=${reusable.destroyed}`);
for await (const record of reusable) {
  console.log(`remaining=${record}`);
}
