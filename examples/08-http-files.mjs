import { createWriteStream } from 'node:fs';
import { mkdir, mkdtemp, open, readFile, rename, rm } from 'node:fs/promises';
import { createServer } from 'node:http';
import { join, resolve } from 'node:path';
import { Transform } from 'node:stream';
import { pipeline } from 'node:stream/promises';
import { setTimeout as delay } from 'node:timers/promises';

const uploadLimit = 64 * 1024;
const uploadDeadline = 2000;
const storage = resolve(process.env.UPLOAD_DIR ?? 'scratch/08-upload');
const demoFile = new URL('../fixtures/events.ndjson', import.meta.url);
const downloadFile = process.env.DOWNLOAD_FILE ?? demoFile;
const port = Number(process.env.PORT ?? 3001);
let uploadInProgress = false;

await mkdir(storage, { recursive: true });

function reply(response, status, body, close = false) {
  const data = JSON.stringify(body) + '\n';
  const headers = {
    'Content-Type': 'application/json; charset=utf-8',
    'Content-Length': Buffer.byteLength(data),
  };
  if (close) headers.Connection = 'close';
  response.writeHead(status, headers).end(data);
}

async function sendDownload(response, source) {
  const controller = new AbortController();
  function disconnected() {
    if (!response.writableFinished) controller.abort();
  }
  response.once('close', disconnected);
  if (response.destroyed) disconnected();

  try {
    await pipeline(source, response, { signal: controller.signal });
    console.log('download finished');
  } catch (error) {
    console.error('download stopped:', error.code ?? error.name);
  } finally {
    response.off('close', disconnected);
  }
}

async function downloadFileResponse(response) {
  let file;
  try {
    file = await open(downloadFile, 'r');
    const info = await file.stat();
    if (!info.isFile()) {
      reply(response, 404, { error: 'file not found' });
      return;
    }

    response.setHeader('Content-Type', 'application/x-ndjson; charset=utf-8');
    response.setHeader('Content-Length', info.size);
    await sendDownload(response, file.createReadStream());
  } catch (error) {
    if (!response.headersSent && !response.destroyed) {
      const status = error.code === 'ENOENT' ? 404 : 500;
      reply(response, status, { error: status === 404 ? 'file not found' : 'download failed' });
    } else {
      response.destroy();
    }
  } finally {
    if (file) {
      await file.close().catch((error) => {
        console.error('download close warning:', error.code ?? error.name);
      });
    }
  }
}

function demoDownload(response, fail) {
  response.writeHead(200, {
    'Content-Type': 'application/octet-stream',
    'Content-Length': 8 * 16_384,
  });
  response.flushHeaders();

  async function* paced({ signal }) {
    try {
      for (let i = 0; i < 8; i += 1) {
        if (i > 0) await delay(180, undefined, { signal });
        if (fail && i === 1) {
          const error = new Error('simulated read failure');
          error.code = 'DEMO_READ';
          throw error;
        }
        yield Buffer.alloc(16_384, i);
      }
    } finally {
      console.log(`download source released aborted=${signal.aborted}`);
    }
  }

  return sendDownload(response, paced);
}

async function upload(request, response) {
  const mediaType = request.headers['content-type']?.split(';', 1)[0].trim().toLowerCase();
  const encoding = request.headers['content-encoding'] ?? 'identity';
  if (mediaType !== 'application/octet-stream' || encoding.toLowerCase() !== 'identity') {
    reply(response, 415, { error: 'expected unencoded application/octet-stream' }, true);
    return;
  }
  if (Number(request.headers['content-length']) > uploadLimit) {
    reply(response, 413, { error: 'upload exceeds 65536 bytes' }, true);
    return;
  }
  if (uploadInProgress) {
    reply(response, 503, { error: 'another upload is in progress' }, true);
    return;
  }
  uploadInProgress = true;
  try {
    await receiveUpload(request, response);
  } finally {
    uploadInProgress = false;
    console.log('upload slot released');
  }
}

async function receiveUpload(request, response) {
  const signal = AbortSignal.timeout(uploadDeadline);
  const directory = await mkdtemp(join(storage, '.incoming-'));
  const partial = join(directory, 'data.bin');
  let bytes = 0;
  const limit = new Transform({
    transform(chunk, _encoding, done) {
      const first = bytes === 0;
      bytes += chunk.length;
      if (bytes > uploadLimit) {
        const error = new Error('upload too large');
        error.code = 'UPLOAD_LIMIT';
        done(error);
        return;
      }
      if (first) console.log('upload received first bytes');
      done(null, chunk);
    },
  });

  try {
    await pipeline(request, limit, createWriteStream(partial, { flags: 'wx' }), { signal });
    signal.throwIfAborted();
    if (response.destroyed) return;
    await rename(partial, join(storage, 'received.bin'));
    reply(response, 201, { bytes });
  } catch (error) {
    const code = error.code ?? error.name;
    console.error('upload stopped:', code, 'deadline=' + signal.aborted);
    if (!response.headersSent && !response.destroyed) {
      const status = code === 'UPLOAD_LIMIT' ? 413 : signal.aborted ? 408 : 500;
      reply(response, status, { error: 'upload not committed' }, true);
    }
  } finally {
    await rm(directory, { force: true, recursive: true }).catch((error) => {
      console.error('upload cleanup warning:', error.code ?? error.name);
    });
  }
}

async function route(request, response) {
  if (request.method === 'GET' && request.url === '/health') {
    reply(response, 200, { ok: true });
  } else if (request.method === 'GET' && request.url === '/download') {
    await downloadFileResponse(response);
  } else if (request.method === 'GET' && request.url === '/download-changed') {
    const original = await readFile(demoFile, 'utf8');
    const changed = original.replace('"demo-01"', '"demo-10"');
    response.writeHead(200, {
      'Content-Type': 'application/x-ndjson; charset=utf-8',
      'Content-Length': Buffer.byteLength(changed),
    }).end(changed);
  } else if (request.method === 'GET' && request.url === '/download-slow') {
    await demoDownload(response, false);
  } else if (request.method === 'GET' && request.url === '/download-fail') {
    await demoDownload(response, true);
  } else if (request.method === 'POST' && request.url === '/upload') {
    await upload(request, response);
  } else {
    reply(response, 404, { error: 'route not found' }, true);
  }
}

const server = createServer((request, response) => {
  route(request, response).catch((error) => {
    console.error('request failed:', error.code ?? error.name);
    if (!response.headersSent && !response.destroyed) {
      reply(response, 500, { error: 'request failed' }, true);
    } else {
      response.destroy();
    }
  });
});
server.listen(port, '127.0.0.1', () => {
  console.log('Listening on http://127.0.0.1:' + server.address().port);
});
