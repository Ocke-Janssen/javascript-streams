import { createServer } from 'node:http';
import { pipeline } from 'node:stream/promises';
import { setTimeout as delay } from 'node:timers/promises';

async function* rows({ signal }) {
  try {
    yield 'id,status\n';
    for (let id = 1; id <= 3; id += 1) {
      await delay(1_000, undefined, { signal });
      yield `${id},ready\n`;
    }
  } finally {
    console.log('source released aborted=' + signal.aborted);
  }
}

async function report(response) {
  const controller = new AbortController();
  function onClose() {
    if (!response.writableFinished) controller.abort();
  }

  response.once('close', onClose);
  try {
    response.setHeader('Content-Type', 'text/csv; charset=utf-8');
    await pipeline(rows, response, {
      signal: controller.signal,
    });
  } catch (error) {
    console.error('report stopped:', error.code ?? error.name);
  } finally {
    response.off('close', onClose);
  }
}

const server = createServer((request, response) => {
  if (request.method === 'GET' && request.url === '/download-slow') {
    void report(response);
  } else if (request.method === 'GET' && request.url === '/health') {
    response.setHeader('Content-Type', 'application/json');
    response.end('{"ok":true}');
  } else {
    response.writeHead(404).end();
  }
});
const port = Number(process.env.PORT ?? 3001);
server.listen(port, '127.0.0.1', () => {
  console.log('Listening on http://127.0.0.1:' + server.address().port);
});
