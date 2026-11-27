const counters = ['recordsRead', 'kept', 'dropped'];
const runIdPattern = /^[a-f0-9]{8}(?:-[a-f0-9]{4}){3}-[a-f0-9]{12}$/;

async function* readEvents(body) {
  let pending = '';
  for await (const text of body.pipeThrough(new TextDecoderStream('utf-8', { fatal: true }))) {
    pending += text;
    let newline;
    while ((newline = pending.indexOf('\n')) !== -1) {
      if (newline > 4096) throw new Error('Event too large');
      let event;
      try {
        event = JSON.parse(pending.slice(0, newline));
      } catch {
        throw new Error('Invalid progress JSON');
      }
      yield event;
      pending = pending.slice(newline + 1);
    }
    if (pending.length > 4096) throw new Error('Event too large');
  }
  if (pending.length > 0) throw new Error('Incomplete event');
}

// Yield progress immediately. The final event is usable only after normal EOF.
export async function* readProgress(body) {
  let runId;
  let previous = [0, 0, 0];
  let terminal;
  for await (const event of readEvents(body)) {
    if (terminal) throw new Error('Event after run.end');
    if (!event || typeof event.runId !== 'string' || !runIdPattern.test(event.runId)) {
      throw new Error('Missing or invalid run ID');
    }
    if (!runId) {
      if (event.event !== 'run.start') throw new Error('Expected run.start');
      runId = event.runId;
      yield event;
      continue;
    }
    if (event.runId !== runId) throw new Error('Unexpected run ID');
    if (event.event !== 'run.progress' && event.event !== 'run.end') {
      throw new Error('Unexpected event type');
    }
    const next = counters.map((name) => event[name]);
    if (next.some((value, index) => !Number.isSafeInteger(value) || value < previous[index]) ||
        event.kept + event.dropped !== event.recordsRead) {
      throw new Error('Invalid record counters');
    }
    previous = next;
    if (event.event === 'run.progress') yield event;
    else terminal = event;
  }
  if (!terminal) throw new Error('Missing end event');
  if (!['ok', 'aborted', 'error'].includes(terminal.status)) throw new Error('Invalid end status');
  if (terminal.status === 'ok' && (terminal.outputPath !== '/output/' + runId + '.ndjson.gz' ||
      typeof terminal.outputSha256 !== 'string' || !/^[a-f0-9]{64}$/.test(terminal.outputSha256))) {
    throw new Error('Invalid completion metadata');
  }
  yield terminal;
}
