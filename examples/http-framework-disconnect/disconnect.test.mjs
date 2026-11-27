import assert from 'node:assert/strict';
import { EventEmitter, once } from 'node:events';
import { createServer, get } from 'node:http';
import { Readable } from 'node:stream';
import { pipeline } from 'node:stream/promises';
import test from 'node:test';

import express from 'express';
import Fastify from 'fastify';

const first = '{"id":1}\n';
const second = '{"id":2}\n';
const contentType = 'application/x-ndjson';
const host = '127.0.0.1';

function exportSource({ forwardSignal, pending, gate, trace, cleaned }, signal) {
  async function* records() {
    try {
      yield first;
      const next = once(gate, 'release', { signal: forwardSignal ? signal : undefined });
      trace.push('waiting');
      pending.resolve();
      try {
        await next;
      } catch (error) {
        if (signal.aborted && error.code === 'ABORT_ERR') return;
        throw error;
      }
      trace.push('second-produced');
      yield second;
    } finally {
      trace.push('source-released');
      cleaned.resolve();
    }
  }

  return Readable.from(records(), { objectMode: false });
}

function responseSignal(response, state) {
  const controller = new AbortController();
  response.once('close', () => {
    const early = !response.writableFinished;
    state.trace.push(early ? 'response-closed-early' : 'response-finished');
    if (early) controller.abort();
    state.closed.resolve({ early, aborted: controller.signal.aborted });
  });
  return controller.signal;
}

async function startExpress(t, state) {
  const app = express();
  app.get('/export', async (_request, response) => {
    const signal = responseSignal(response, state);
    const source = exportSource(state, signal);
    response.set('Content-Type', contentType);
    try {
      await pipeline(source, response, { signal });
    } catch (error) {
      if (!signal.aborted || !['ABORT_ERR', 'ERR_STREAM_PREMATURE_CLOSE'].includes(error.code)) {
        throw error;
      }
    }
  });
  app.get('/health', (_request, response) => response.send('ok'));

  const server = createServer(app);
  server.listen(0, host);
  t.after(() => new Promise((resolve, reject) => {
    server.close((error) => error ? reject(error) : resolve());
  }));
  await once(server, 'listening');
  return `http://${host}:${server.address().port}`;
}

async function startFastify(t, state) {
  const app = Fastify();
  app.get('/export', (_request, reply) => {
    const signal = responseSignal(reply.raw, state);
    const source = exportSource(state, signal);
    return reply.type(contentType).send(source);
  });
  app.get('/health', () => 'ok');
  t.after(() => app.close());
  return app.listen({ port: 0, host });
}

function disconnectAfterFirstLine(url, signal) {
  return new Promise((resolve, reject) => {
    const request = get(url, { signal }, (response) => {
      let body = '';
      response.setEncoding('utf8');
      response.on('data', (text) => {
        body += text;
        if (body.includes('\n')) response.destroy();
      });
      response.once('error', reject);
      response.once('close', () => resolve({
        status: response.statusCode,
        body,
        complete: response.complete,
      }));
    });
    request.once('error', reject);
  });
}

function newExport(t, forwardSignal) {
  const state = {
    forwardSignal,
    gate: new EventEmitter(),
    pending: Promise.withResolvers(),
    closed: Promise.withResolvers(),
    cleaned: Promise.withResolvers(),
    trace: [],
  };
  t.after(() => { state.gate.emit('release'); });
  return state;
}

for (const [framework, start] of [['Express', startExpress], ['Fastify', startFastify]]) {
  for (const forwardSignal of [false, true]) {
    const variant = forwardSignal ? 'signal forwarded' : 'signal missing in producer';
    test(`${framework}: client disconnects, ${variant}`, { timeout: 5000 }, async (t) => {
      const state = newExport(t, forwardSignal);
      const base = await start(t, state);
      const [received, closed] = await Promise.all([
        disconnectAfterFirstLine(`${base}/export`, t.signal),
        state.closed.promise,
        state.pending.promise,
      ]);

      assert.deepEqual(received, { status: 200, body: first, complete: false });
      assert.deepEqual(closed, { early: true, aborted: true });
      const expected = ['waiting', 'response-closed-early'];
      if (!forwardSignal) {
        assert.deepEqual(state.trace, expected);
        state.gate.emit('release');
        expected.push('second-produced');
      }
      await state.cleaned.promise;
      assert.deepEqual(state.trace, [...expected, 'source-released']);

      const health = await fetch(`${base}/health`, { signal: t.signal });
      assert.equal(health.status, 200);
      assert.equal(await health.text(), 'ok');
      t.diagnostic(`${state.trace.join(' → ')} → health=200`);
    });
  }

  test(`${framework}: normal completion does not abort the producer`, { timeout: 5000 }, async (t) => {
    const state = newExport(t, true);
    const base = await start(t, state);
    const result = fetch(`${base}/export`, { signal: t.signal });
    const response = await result;
    await state.pending.promise;
    state.gate.emit('release');
    assert.equal(response.status, 200);
    assert.equal(await response.text(), first + second);
    assert.deepEqual(await state.closed.promise, { early: false, aborted: false });
    assert.deepEqual(state.trace, ['waiting', 'second-produced', 'source-released', 'response-finished']);
  });
}
