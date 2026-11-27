import assert from 'node:assert/strict';
import { test } from 'node:test';
import { copy } from './limit-copy.mjs';

test('copies four bytes below the limit', async () => {
  const chunks = [Buffer.from('abcd')];
  assert.deepEqual(await copy(chunks, 5), Buffer.from('abcd'));
});

test('copies exactly five bytes', async () => {
  const chunks = [Buffer.from('ab'), Buffer.from('cde')];
  assert.deepEqual(await copy(chunks, 5), Buffer.from('abcde'));
});

test('rejects six bytes', async () => {
  const chunks = [Buffer.from('abcde'), Buffer.from('f')];
  await assert.rejects(copy(chunks, 5), { message: 'input_limit' });
});
