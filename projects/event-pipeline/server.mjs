import { randomUUID } from 'node:crypto';
import { once } from 'node:events';
import { createReadStream } from 'node:fs';
import { mkdir, readFile, realpath, stat } from 'node:fs/promises';
import { createServer } from 'node:http';
import { join, resolve } from 'node:path';
import { pipeline } from 'node:stream/promises';
import { setTimeout as delay } from 'node:timers/promises';
import { fileURLToPath } from 'node:url';
import { buildOutput, limits, newCounters, PipelineError } from './pipeline.mjs';

const assets = new Map([
  ['/', ['public/index.html', 'text/html; charset=utf-8']],
  ['/app.mjs', ['public/app.mjs', 'text/javascript; charset=utf-8']],
  ['/progress.mjs', ['public/progress.mjs', 'text/javascript; charset=utf-8']],
]);

function reply(res, code, reason) {
  res.writeHead(code, { 'Content-Type': 'application/json; charset=utf-8' });
  res.end(JSON.stringify({ reason }) + '\n');
}

export async function createDemoServer({
  workspace = 'scratch/event-pipeline',
  log = (entry) => console.log(JSON.stringify(entry)),
  slowWrite = (signal) => delay(650, undefined, { signal }),
  deadlineMs = limits.deadlineMs,
  publish,
} = {}) {
  const outputDir = resolve(workspace, 'output');
  await mkdir(outputDir, { recursive: true });
  let activeJob;
  let shuttingDown = false;
  let shutdownPromise;
  const downloads = new Set();

  async function runRequest(res, mode, input, controller) {
    const { signal } = controller;
    const counters = newCounters();
    const runId = randomUUID();
    const started = performance.now();
    const filename = runId + '.ndjson.gz';
    function onClose() {
      if (!res.writableFinished) controller.abort(new PipelineError('client_disconnect'));
    }
    function event(name, fields) {
      const entry = { event: name, runId, elapsedMs: Math.round(performance.now() - started), ...fields };
      log(entry);
      return entry;
    }
    async function send(entry, sendSignal = signal) {
      sendSignal.throwIfAborted();
      if (!res.write(JSON.stringify(entry) + '\n')) {
        await once(res, 'drain', { signal: sendSignal });
      }
    }

    res.once('close', onClose);
    const timer = setTimeout(() => controller.abort(new PipelineError('deadline')), deadlineMs);
    timer.unref();
    try {
      res.writeHead(200, {
        'Content-Type': 'application/x-ndjson; charset=utf-8',
        'Cache-Control': 'no-store',
      });
      let result = { status: 'ok', reason: 'completed' };
      try {
        await send(event('run.start', { mode, input }));
        const inputFile = mode === 'bad'
          ? new URL('fixtures/invalid.ndjson', import.meta.url)
          : input === 'large'
            ? resolve(workspace, 'input.ndjson')
            : new URL('fixtures/demo.ndjson', import.meta.url);
        const output = await buildOutput({
          openSource: () => createReadStream(inputFile, { highWaterMark: 16 * 1024 }),
          output: join(outputDir, filename),
          signal, counters, publish,
          progress: (counts, pipeSignal) => send(event('run.progress', counts), pipeSignal),
          beforeWrite: mode === 'slow' ? slowWrite : undefined,
        });
        result = { ...result, ...output, outputPath: '/output/' + filename };
      } catch (error) {
        if (signal.aborted && (error === signal.reason || error.code === 'ABORT_ERR')) {
          const reason = signal.reason.code;
          result = { status: reason === 'deadline' ? 'error' : 'aborted', reason };
        } else {
          result = { status: 'error', reason: error instanceof PipelineError ? error.code : 'io_error' };
          if (error instanceof PipelineError && error.line !== undefined) result.line = error.line;
        }
      }
      const end = event('run.end', { ...counters, ...result, sinkWaitMs: Math.round(counters.sinkWaitMs) });
      if (!res.destroyed) res.end(JSON.stringify(end) + '\n');
    } finally {
      clearTimeout(timer);
      res.off('close', onClose);
    }
  }

  async function route(req, res) {
    const host = '127.0.0.1:' + req.socket.localPort;
    const origin = 'http://' + host;
    if (req.headers.host !== host) return reply(res, 403, 'invalid_host');
    const url = new URL(req.url, origin);
    if (req.method === 'GET' && assets.has(url.pathname)) {
      const [file, type] = assets.get(url.pathname);
      const bytes = await readFile(new URL(file, import.meta.url));
      res.writeHead(200, { 'Content-Type': type });
      res.end(bytes);
      return;
    }
    if (req.method === 'GET' && /^\/output\/[a-f0-9-]{36}\.ndjson\.gz$/.test(url.pathname)) {
      if (shuttingDown) {
        res.setHeader('Connection', 'close');
        return reply(res, 503, 'shutting_down');
      }
      const controller = new AbortController();
      downloads.add(controller);
      try {
        const file = join(outputDir, url.pathname.slice('/output/'.length));
        const info = await stat(file).catch((error) => {
          if (error.code !== 'ENOENT') throw error;
        });
        if (!info) return reply(res, 404, 'missing_output');
        res.writeHead(200, {
          'Content-Type': 'application/gzip',
          'Content-Length': info.size,
        });
        await pipeline(createReadStream(file), res, { signal: controller.signal });
        return;
      } finally {
        downloads.delete(controller);
      }
    }
    if (req.method !== 'POST' || url.pathname !== '/run') return reply(res, 404, 'not_found');
    if (req.headers['x-stream-demo'] !== '1' ||
        (req.headers.origin && req.headers.origin !== origin)) {
      return reply(res, 403, 'invalid_origin');
    }
    const mode = url.searchParams.get('mode') ?? 'ok';
    const input = url.searchParams.get('input') ?? 'demo';
    if (!['ok', 'slow', 'bad'].includes(mode) || !['demo', 'large'].includes(input) ||
        (mode !== 'ok' && input !== 'demo') || req.headers['transfer-encoding'] ||
        (req.headers['content-length'] && req.headers['content-length'] !== '0')) {
      return reply(res, 400, 'invalid_run');
    }
    if (shuttingDown) {
      res.setHeader('Connection', 'close');
      return reply(res, 503, 'shutting_down');
    }
    if (activeJob) return reply(res, 409, 'busy');
    const job = { controller: new AbortController(), done: Promise.withResolvers() };
    activeJob = job;
    try {
      await runRequest(res, mode, input, job.controller);
    } finally {
      activeJob = undefined;
      job.done.resolve();
    }
  }

  const server = createServer((req, res) => {
    route(req, res).catch(() => {
      if (!res.headersSent) reply(res, 500, 'server_error');
      else res.destroy();
    });
  });

  // Opt-in lifecycle for S16-R. Ordinary demo startup and close() remain available.
  server.shutdown = ({ graceMs = 1000 } = {}) => {
    if (shutdownPromise) return shutdownPromise;
    if (!Number.isSafeInteger(graceMs) || graceMs < 0 || graceMs > 2_147_483_647) {
      throw new RangeError('graceMs must be an integer from 0 through 2147483647');
    }
    shuttingDown = true;
    const job = activeJob;
    let graceExpired = false;
    log({ event: 'server.draining', graceMs, activeRun: Boolean(job), activeDownloads: downloads.size });
    const timer = setTimeout(() => {
      graceExpired = true;
      log({ event: 'server.shutdown_deadline' });
      job?.controller.abort(new PipelineError('shutdown_deadline'));
      for (const controller of downloads) {
        controller.abort(new PipelineError('shutdown_deadline'));
      }
    }, graceMs);
    shutdownPromise = (async () => {
      try {
        // Keep listening while the accepted job drains: a new /run can observe 503.
        // An initiated publish is awaited even if its signal has since been aborted.
        await job?.done.promise;
        await new Promise((resolve, reject) => server.close((error) => {
          if (error && error.code !== 'ERR_SERVER_NOT_RUNNING') reject(error);
          else resolve();
        }));
        const result = { graceExpired };
        log({ event: 'server.stopped', ...result });
        return result;
      } finally {
        clearTimeout(timer);
      }
    })();
    return shutdownPromise;
  };
  return server;
}

const entry = process.argv[1];
const modulePath = fileURLToPath(import.meta.url);
if (entry && (resolve(entry) === modulePath ||
    await realpath(entry).catch(() => null) === modulePath)) {
  const server = await createDemoServer();
  server.listen(3004, '127.0.0.1', () => {
    console.log(JSON.stringify({ event: 'server.ready', url: 'http://127.0.0.1:3004/' }));
  });
}
