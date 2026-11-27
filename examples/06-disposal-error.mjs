import assert from 'node:assert/strict';

const controller = new AbortController();
const signal = controller.signal;
const opened = Promise.withResolvers();

async function inspectInput() {
  signal.throwIfAborted();
  console.log('open pending');
  await using file = await opened.promise;
  console.log('resource received');
  signal.throwIfAborted();
  return await file.stat();
}

const result = inspectInput();
controller.abort();
console.log('abort requested');
opened.resolve({
  async stat() { assert.fail('unexpected stat'); },
  async [Symbol.asyncDispose]() {
    console.log('dispose started');
    throw new Error('simulated close failure');
  },
});

await assert.rejects(result, (error) => {
  assert.ok(error instanceof SuppressedError);
  assert.equal(error.error.message, 'simulated close failure');
  assert.equal(error.suppressed, signal.reason);
  assert.equal(error.suppressed.name, 'AbortError');
  console.log(`result=${error.name}`);
  console.log(`cleanup=${error.error.message}`);
  console.log(`original=${error.suppressed.name}`);
  return true;
});
