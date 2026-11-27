import { get } from 'node:http';

const port = Number(process.env.PORT ?? 3001);
const base = `http://127.0.0.1:${port}`;

await new Promise((resolve, reject) => {
  let received = false;
  const request = get(base + '/download-slow', (response) => {
    response.once('error', (error) => {
      if (!received) reject(error);
    });
    response.once('close', () => {
      if (received) resolve();
      else reject(new Error('download closed before receiving bytes'));
    });
    if (response.statusCode !== 200) {
      reject(new Error(`download status=${response.statusCode}`));
      response.destroy();
      return;
    }

    response.once('data', (chunk) => {
      received = true;
      console.log(`firstBytes=${chunk.length}; closing client`);
      response.destroy();
    });
  });
  request.on('error', reject);
  request.setTimeout(3000, () => request.destroy(new Error('download timed out')));
});

const health = await fetch(base + '/health', { signal: AbortSignal.timeout(3000) });
if (!health.ok || !(await health.json()).ok) throw new Error('health check failed');
console.log(`follow-up status=${health.status}`);
