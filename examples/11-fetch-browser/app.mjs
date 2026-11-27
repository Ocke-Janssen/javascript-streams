import { ndjsonRecords } from './records.mjs';

const startButton = document.querySelector('#start');
const abortButton = document.querySelector('#abort');
const scenario = document.querySelector('#scenario');
const status = document.querySelector('#status');
const list = document.querySelector('#records');

const maxRows = 12;
const batchSize = new URL(location.href).searchParams.get('batch') === '1' ? 1 : 4;
const batchDelayMs = 75;
let active;

function clearBatchSchedule(run) {
  if (run.frame !== undefined) {
    cancelAnimationFrame(run.frame);
    run.frame = undefined;
  }
  if (run.batchTimer !== undefined) {
    clearTimeout(run.batchTimer);
    run.batchTimer = undefined;
  }
}

function flushBatch(run) {
  clearBatchSchedule(run);
  if (active !== run || run.pending.length === 0) return;

  const rows = document.createDocumentFragment();
  for (const record of run.pending.splice(0)) {
    const row = document.createElement('li');
    row.textContent = `#${record.id} ${record.message}`;
    rows.append(row);
  }
  list.append(rows);
  while (list.children.length > maxRows) list.firstElementChild.remove();
  run.commits += 1;
  list.dataset.renderCommits = String(run.commits);
}

function render(run) {
  return new WritableStream({
    write(record) {
      if (active !== run) return;
      run.count += 1;
      run.pending.push(record);
      if (run.pending.length >= batchSize) {
        flushBatch(run);
      } else if (run.frame === undefined) {
        run.frame = requestAnimationFrame(() => flushBatch(run));
        run.batchTimer = setTimeout(() => flushBatch(run), batchDelayMs);
      }
    },
  });
}

function discardRun(run) {
  clearBatchSchedule(run);
  run.pending.length = 0;
}

function stop() {
  if (!active) return;
  const run = active;
  active = undefined;
  discardRun(run);
  run.controller.abort();
  abortButton.disabled = true;
  const shown = list.childElementCount;
  const unit = shown === 1 ? 'record' : 'records';
  status.textContent = `Aborted (${shown} ${unit} shown)`;
}

async function start() {
  stop();
  const run = { controller: new AbortController(), pending: [], count: 0, commits: 0 };
  active = run;
  list.replaceChildren();
  list.dataset.renderCommits = '0';
  status.textContent = 'Connecting';
  abortButton.disabled = false;

  const { signal } = run.controller;
  try {
    const response = await fetch(`/api/feed?case=${scenario.value}`, { signal });
    if (active !== run) {
      await response.body?.cancel().catch(() => {});
      return;
    }

    if (!response.ok) {
      await response.body?.cancel().catch(() => {});
      throw new Error(`HTTP ${response.status}`);
    }
    if (response.status === 204 || response.status === 205 || !response.body) {
      await response.body?.cancel().catch(() => {});
      throw new Error('response has no body');
    }

    status.textContent = 'Streaming';
    const text = response.body.pipeThrough(new TextDecoderStream('utf-8', { fatal: true }));
    const records = ndjsonRecords(text);
    await records.pipeTo(render(run), { signal });

    if (active !== run) return;
    flushBatch(run);
    status.textContent = `Complete (${run.count} records)`;
  } catch (error) {
    if (active !== run) return;
    flushBatch(run);
    status.textContent = `Error: ${error.message}`;
  } finally {
    if (active === run) {
      const abortWasFocused = document.activeElement === abortButton;
      active = undefined;
      abortButton.disabled = true;
      if (abortWasFocused) startButton.focus();
    }
  }
}

startButton.addEventListener('click', start);
abortButton.addEventListener('click', () => {
  stop();
  startButton.focus();
});
window.addEventListener('pagehide', stop);
