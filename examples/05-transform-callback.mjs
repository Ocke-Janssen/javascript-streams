import assert from 'node:assert/strict';
import { PassThrough, Transform, Writable } from 'node:stream';
import { pipeline } from 'node:stream/promises';

async function compare(acknowledgeEarly) {
  const input = new PassThrough({ objectMode: true });
  const releaseA = Promise.withResolvers();
  const receivedB = Promise.withResolvers();
  const receivedBoth = Promise.withResolvers();
  const started = [];
  const output = [];
  const enrich = new Transform({
    objectMode: true,
    transform(id, _encoding, done) {
      started.push(id);
      const lookup = id === 'A' ? releaseA.promise : Promise.resolve();
      if (acknowledgeEarly) {
        // This variant deliberately acknowledges input before its work ends.
        done();
        lookup.then(() => this.push(id));
      } else {
        lookup.then(() => done(null, id), done);
      }
    },
  });
  const sink = new Writable({
    objectMode: true,
    write(id, _encoding, done) {
      output.push(id);
      if (id === 'B') receivedB.resolve();
      if (output.length === 2) receivedBoth.resolve();
      done();
    },
  });
  const deadline = new AbortController();
  const completed = pipeline(input, enrich, sink, { signal: deadline.signal });
  const deadlineTimer = setTimeout(() => {
    deadline.abort(new Error('Transform probe did not reach its next gate'));
  }, 5_000);

  try {
    input.write('A');
    input.write('B');
    if (acknowledgeEarly) await Promise.race([receivedB.promise, completed]);
    const startedBeforeRelease = started.join(',');
    const outputBeforeRelease = output.join(',') || 'none';
    releaseA.resolve();
    // Keep the source open to isolate the ordering bug from premature EOF.
    await Promise.race([receivedBoth.promise, completed]);

    assert.equal(startedBeforeRelease, acknowledgeEarly ? 'A,B' : 'A');
    assert.equal(outputBeforeRelease, acknowledgeEarly ? 'B' : 'none');
    assert.deepEqual(output, acknowledgeEarly ? ['B', 'A'] : ['A', 'B']);
    console.log(
      `callback=${acknowledgeEarly ? 'early' : 'after-result'} ` +
      `startedBeforeRelease=${startedBeforeRelease} outputBeforeRelease=${outputBeforeRelease} ` +
      `final=${output.join(',')}`,
    );
  } finally {
    releaseA.resolve();
    input.end();
    try {
      await completed;
    } finally {
      clearTimeout(deadlineTimer);
    }
  }
}

await compare(false);
await compare(true);
