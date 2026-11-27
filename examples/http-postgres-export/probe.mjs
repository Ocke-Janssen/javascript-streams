import assert from 'node:assert/strict';
import { EventEmitter, once } from 'node:events';
import { createServer } from 'node:http';
import { setTimeout as delay } from 'node:timers/promises';
import { Pool } from 'pg';
import { exportCsv } from './export.mjs';

const rowsSql = `
  SELECT id, 'event-' || id AS event
  FROM generate_series(1, 3) AS events(id)
  ORDER BY id
`;
const slowRowsSql = `
  SELECT id, 'event-' || id AS event,
         CASE WHEN id = 2 THEN (SELECT 1 FROM pg_sleep(20)) END AS pause
  FROM generate_series(1, 3) AS events(id)
`;
const expectedCsv = 'id,event\n1,event-1\n2,event-2\n3,event-3\n';

const poolOptions = { max: 1, connectionTimeoutMillis: 2_000, statement_timeout: 3_000 };
const pool = new Pool(poolOptions);
const observer = new Pool(poolOptions);
const traces = new EventEmitter();
const server = createServer(async (request, response) => {
  const sql = request.url === '/slow.csv' ? slowRowsSql : rowsSql;
  traces.emit('settled', await exportCsv(pool, response, sql));
});

async function firstRecord(reader) {
  const decoder = new TextDecoder();
  let prefix = '';
  while (prefix.split('\n').length < 3) {
    const { value, done } = await reader.read();
    assert.equal(done, false, 'response must contain the first record');
    prefix += decoder.decode(value, { stream: true });
  }
  assert.equal(prefix, 'id,event\n1,event-1\n');
}

async function waitForDatabaseRead(pid) {
  const deadline = AbortSignal.timeout(2_000);
  while (!deadline.aborted) {
    const { rows } = await observer.query(
      'SELECT wait_event FROM pg_stat_activity WHERE pid = $1',
      [pid],
    );
    if (rows[0]?.wait_event === 'PgSleep') return;
    await delay(20, undefined, { signal: deadline });
  }
  throw new Error('The second cursor read was not observed');
}

function nextTrace() {
  return once(traces, 'settled', { signal: AbortSignal.timeout(11_000) });
}

async function checkCompleteExport(base) {
  const complete = await fetch(`${base}/events.csv`, { signal: AbortSignal.timeout(10_000) });
  assert.equal(complete.status, 200);
  assert.equal(await complete.text(), expectedCsv);
}

async function interruptExport(base, pid) {
  const abort = new AbortController();
  const signal = AbortSignal.any([abort.signal, AbortSignal.timeout(10_000)]);
  const interrupted = await fetch(`${base}/slow.csv`, { signal });
  assert.equal(interrupted.status, 200);
  const reader = interrupted.body.getReader();
  try {
    await firstRecord(reader);
    await waitForDatabaseRead(pid);
    console.log('interrupted status=200 firstRecord=true databaseReadPending=true');
  } finally {
    abort.abort();
    reader.releaseLock();
  }
}

try {
  const { rows: [{ pid }] } = await pool.query('SELECT pg_backend_pid() AS pid');
  server.listen(0, '127.0.0.1');
  await once(server, 'listening');
  const base = `http://127.0.0.1:${server.address().port}`;

  const [[completed]] = await Promise.all([nextTrace(), checkCompleteExport(base)]);
  assert.equal(completed.errorCode, undefined);
  assert.equal(completed.cursorClosed, true);
  assert.equal(pool.idleCount, 1);
  console.log('complete status=200 csvMatches=true cursorClosed=true connection=returned');

  const [[stopped]] = await Promise.all([nextTrace(), interruptExport(base, pid)]);
  assert.equal(stopped.responseClosedEarly, true);
  assert.equal(stopped.cursorClosed, true);
  assert.equal(stopped.connectionDiscarded, true);
  assert.equal(pool.totalCount, 0);
  console.log('cleanup responseClosedEarly=true cursorClosed=true connection=discarded');

  const { rows: [followUp] } = await pool.query('SELECT 1 AS ok, pg_backend_pid() AS pid');
  assert.equal(followUp.ok, 1);
  assert.notEqual(followUp.pid, pid);
  console.log('follow-up database=1 newConnection=true');
} finally {
  if (server.listening) await new Promise((resolve) => server.close(resolve));
  await Promise.all([pool.end(), observer.end()]);
}
