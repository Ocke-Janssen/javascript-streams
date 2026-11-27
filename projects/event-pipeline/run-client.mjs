import { once } from 'node:events';
import { request } from 'node:http';
import { Readable } from 'node:stream';
import { pipeline } from 'node:stream/promises';
import { readProgress } from './public/progress.mjs';

const argumentsFromShell = process.argv.slice(2);
const checked = argumentsFromShell.at(-1) === '--check';
if (checked) argumentsFromShell.pop();
const [mode = 'slow', input = 'demo', ...extra] = argumentsFromShell;
if (!['ok', 'slow', 'bad'].includes(mode) || !['demo', 'large'].includes(input) ||
    (mode !== 'ok' && input !== 'demo') || extra.length > 0) {
  console.error('Usage: node projects/event-pipeline/run-client.mjs [ok|slow|bad] [demo|large] [--check]');
  process.exitCode = 1;
} else {
  const controller = new AbortController();
  const stop = () => controller.abort();
  process.once('SIGINT', stop);
  try {
    const url = new URL('http://127.0.0.1:3004/run');
    url.search = new URLSearchParams({ mode, input });
    const req = request(url, {
      method: 'POST', headers: { 'X-Stream-Demo': '1' }, signal: controller.signal,
    });
    const response = once(req, 'response');
    req.end();
    const [body] = await response;
    console.error('HTTP ' + body.statusCode);
    if (checked && body.statusCode === 200) {
      const events = readProgress(Readable.toWeb(body));
      async function* checkedLines() {
        for await (const event of events) {
          if (event.event === 'run.end' && event.status !== 'ok') process.exitCode = 1;
          yield JSON.stringify(event) + '\n';
        }
      }
      await pipeline(checkedLines(), process.stdout, { end: false, signal: controller.signal });
    } else {
      await pipeline(body, process.stdout, { end: false, signal: controller.signal });
      if (body.statusCode !== 200) process.exitCode = 1;
    }
  } catch (error) {
    const failure = checked ? 'Run not verified: ' : 'Transfer failed: ';
    console.error(controller.signal.aborted ? 'Client cancelled' : failure + error.message);
    process.exitCode = 1;
  } finally {
    process.off('SIGINT', stop);
  }
}
