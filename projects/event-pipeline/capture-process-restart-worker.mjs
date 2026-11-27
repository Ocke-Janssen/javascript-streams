import { EventEmitter, once } from 'node:events';
import { createDemoServer } from './server.mjs';

const [workspace, scenario, port] = process.argv.slice(2);
if (!process.connected || !workspace || !['within-grace', 'before-publication', 'replacement'].includes(scenario)) {
  throw new Error('Start this worker through capture-process-restart.mjs');
}

const stop = Promise.withResolvers();
process.once('SIGTERM', () => stop.resolve('SIGTERM'));
process.once('disconnect', () => stop.resolve('disconnect'));

function report(entry) {
  if (!process.connected) return;
  process.send(entry, (error) => {
    if (error && process.connected) {
      console.error(error);
      process.exitCode = 1;
      stop.resolve('ipc_error');
    }
  });
}

const writeGate = new EventEmitter();
let writeReleased = false;
const server = await createDemoServer({
  workspace,
  log(entry) {
    report(entry);
    if (entry.event === 'server.draining' && scenario === 'within-grace') {
      writeReleased = true;
      writeGate.emit('release');
    }
  },
  async slowWrite(signal) {
    if (writeReleased) return;
    report({ event: 'probe.write_waiting' });
    await once(writeGate, 'release', { signal });
  },
});

try {
  server.listen(Number(port), '127.0.0.1');
  await once(server, 'listening');
  report({ event: 'probe.ready', pid: process.pid, port: server.address().port });
  report({ event: 'probe.stopping', trigger: await stop.promise });
  await server.shutdown({ graceMs: scenario === 'within-grace' ? 1000 : 0 });
} finally {
  if (process.connected) process.disconnect();
}
