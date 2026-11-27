import { readProgress } from './progress.mjs';

const form = document.querySelector('form');
const scenario = document.querySelector('#scenario');
const start = document.querySelector('#start');
const stop = document.querySelector('#stop');
const download = document.querySelector('#download');
let controller;

function show(id, value) {
  const element = document.getElementById(id);
  const text = String(value);
  if (element.textContent !== text) element.textContent = text;
}

function render(entry) {
  show('run-id', entry.runId);
  if (entry.event === 'run.progress' || entry.event === 'run.end') {
    show('read', entry.recordsRead);
    show('kept', entry.kept);
    show('dropped', entry.dropped);
  }
  if (entry.event === 'run.progress') show('status', 'Processing');
  if (entry.event === 'run.end') {
    if (entry.status === 'ok') {
      show('status', 'Complete');
      show('digest', entry.outputSha256);
      download.href = entry.outputPath;
      download.hidden = false;
    } else {
      const location = entry.line === undefined ? '' : ' (line ' + entry.line + ')';
      show('status', 'Stopped: ' + entry.reason + location);
    }
  }
}

form.addEventListener('submit', async (event) => {
  event.preventDefault();
  if (controller) return;
  controller = new AbortController();
  const moveFocus = document.activeElement === start;
  start.disabled = true;
  scenario.disabled = true;
  stop.disabled = false;
  if (moveFocus) stop.focus();
  download.hidden = true;
  download.removeAttribute('href');
  for (const id of ['read', 'kept', 'dropped']) show(id, 0);
  show('run-id', '—');
  show('digest', '—');
  show('status', 'Starting');
  try {
    const input = scenario.value === 'large' ? 'large' : 'demo';
    const mode = input === 'large' ? 'ok' : scenario.value;
    const response = await fetch('/run?' + new URLSearchParams({ mode, input }), {
      method: 'POST', headers: { 'X-Stream-Demo': '1' }, signal: controller.signal,
    });
    if (!response.ok || !response.body) throw new Error('HTTP ' + response.status);
    for await (const entry of readProgress(response.body)) render(entry);
  } catch (error) {
    download.hidden = true;
    download.removeAttribute('href');
    show('digest', '—');
    show('status', controller.signal.aborted ? 'Aborted in browser' : 'Failed: ' + error.message);
  } finally {
    const returnFocus = document.activeElement === stop;
    controller = undefined;
    start.disabled = false;
    scenario.disabled = false;
    if (returnFocus) start.focus();
    stop.disabled = true;
  }
});

stop.addEventListener('click', () => controller?.abort());
