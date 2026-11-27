import assert from 'node:assert/strict';
import { once } from 'node:events';
import { createServer } from 'node:http';

const bytes = Uint8Array.of(0x00, 0x41, 0xff, 0x0a, 0x00);
const arrivals = [];
const server = createServer(async (request, response) => {
  // Record arrival before reading the body, even if the request fails later.
  const arrival = { path: request.url, method: request.method };
  arrivals.push(arrival);

  if (request.method !== 'POST' || !['/start', '/target'].includes(request.url)) {
    response.writeHead(404).end();
    return;
  }

  try {
    const received = Buffer.alloc(bytes.length);
    let length = 0;
    for await (const chunk of request) {
      if (length + chunk.length > received.length) {
        throw new Error('unexpected payload size');
      }
      chunk.copy(received, length);
      length += chunk.length;
    }
    arrival.hex = received.subarray(0, length).toString('hex');

    if (request.url === '/start') {
      arrival.status = 307;
      response.writeHead(307, { Location: '/target' }).end();
    } else {
      arrival.status = 204;
      response.writeHead(204).end();
    }
  } catch (error) {
    response.destroy(error);
  }
});

server.listen(0, '127.0.0.1');
await once(server, 'listening');
const origin = `http://127.0.0.1:${server.address().port}`;

function upload(body, options) {
  return fetch(`${origin}/start`, {
    ...options,
    method: 'POST',
    redirect: 'follow',
    signal: AbortSignal.timeout(5_000),
    body,
  });
}

function printArrival(label, { path, method, hex, status }) {
  const spacedHex = hex.match(/../g).join(' ');
  console.log(`${label} ${path}: ${method} ${spacedHex} -> ${status}`);
}

try {
  const blobResponse = await upload(new Blob([bytes]));
  assert.equal(blobResponse.status, 204);
  assert.equal(blobResponse.redirected, true);
  assert.equal(blobResponse.url, `${origin}/target`);

  const first = { path: '/start', method: 'POST', hex: '0041ff0a00', status: 307 };
  const target = { path: '/target', method: 'POST', hex: '0041ff0a00', status: 204 };
  const blobArrivals = arrivals.splice(0);
  assert.deepEqual(blobArrivals, [first, target]);

  const stream = new ReadableStream({
    start(controller) {
      controller.enqueue(bytes);
      controller.close();
    },
  });
  await assert.rejects(upload(stream, { duplex: 'half' }), {
    name: 'TypeError',
  });
  assert.deepEqual(arrivals, [first]);

  for (const arrival of blobArrivals) printArrival('blob  ', arrival);
  printArrival('stream', arrivals[0]);
  console.log('stream /target: no request (TypeError)');
} finally {
  const closed = once(server, 'close');
  server.close();
  server.closeAllConnections();
  await closed;
}
