import { createServer } from 'node:http';
import { pipeline } from 'node:stream/promises';

const port = Number(process.env.PORT ?? 3000);
const server = createServer(async (request, response) => {
  if (request.method !== 'POST' || request.url !== '/echo') {
    response.writeHead(404).end();
    return;
  }

  response.setHeader('Content-Type', 'application/octet-stream');
  try {
    await pipeline(request, response);
  } catch (error) {
    console.error('Echo interrupted:', error.code ?? error.name);
  }
});

server.listen(port, '127.0.0.1', () => {
  console.log('Listening on http://127.0.0.1:' + server.address().port);
});
