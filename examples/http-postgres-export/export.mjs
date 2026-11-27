import { pipeline } from 'node:stream/promises';
import { setTimeout as delay } from 'node:timers/promises';
import { stringify } from 'csv-stringify';
import QueryStream from 'pg-query-stream';

export async function exportCsv(pool, response, sql) {
  const controller = new AbortController();
  const signal = AbortSignal.any([
    controller.signal,
    AbortSignal.timeout(6_000),
  ]);
  let responseClosedEarly = false;
  let cursorClosed = false;
  let client;
  let reusable = false;
  let errorCode;

  function onResponseClose() {
    if (!response.writableFinished) {
      responseClosedEarly = true;
      controller.abort();
    }
  }

  function onDatabaseError(error) {
    controller.abort(error);
  }

  response.once('close', onResponseClose);
  if (response.destroyed) onResponseClose();

  try {
    client = await pool.connect();
    reusable = true;
    client.on('error', onDatabaseError);
    signal.throwIfAborted();

    const csv = stringify({ header: true, columns: ['id', 'event'] });
    response.setHeader('Content-Type', 'text/csv; charset=utf-8');

    reusable = false;
    const rows = client.query(new QueryStream(sql, [], { batchSize: 1 }));
    const sourceClosed = new Promise((resolve) => {
      rows.once('close', () => {
        cursorClosed = true;
        resolve();
      });
    });
    const transfer = pipeline(rows, csv, response, { signal });

    const cleanupTimer = new AbortController();
    const giveUp = delay(9_000, undefined, {
      ref: false,
      signal: cleanupTimer.signal,
    }).then(() => {
      throw new Error('Database cleanup deadline', { cause: signal.reason });
    });
    try {
      await Promise.race([transfer.finally(() => sourceClosed), giveUp]);
      reusable = true;
    } finally {
      cleanupTimer.abort();
    }
  } catch (error) {
    errorCode = error.code ?? error.name;
    if (!response.headersSent && !response.destroyed) {
      response.writeHead(500, { 'Content-Type': 'text/plain; charset=utf-8' });
      response.end('CSV export failed\n');
    } else if (!response.destroyed) {
      response.destroy();
    }
  } finally {
    response.off('close', onResponseClose);
    if (client) {
      client.off('error', onDatabaseError);
      client.release(!reusable);
    }
  }

  return {
    responseClosedEarly,
    cursorClosed,
    connectionDiscarded: Boolean(client && !reusable),
    errorCode,
  };
}
