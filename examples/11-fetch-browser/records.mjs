const maxLineUnits = 256;

async function* parseRecords(chunks) {
  let pending = '';
  let nextId = 1;

  for await (const text of chunks) {
    let start = 0;
    while (start < text.length) {
      const lf = text.indexOf('\n', start);
      const end = lf === -1 ? text.length : lf;
      if (pending.length + end - start > maxLineUnits) {
        throw new Error('NDJSON line too long');
      }

      pending += text.slice(start, end);
      if (lf === -1) break;

      const line = pending.endsWith('\r') ? pending.slice(0, -1) : pending;
      if (line.includes('\r')) throw new SyntaxError('invalid NDJSON record');

      let record;
      try {
        record = JSON.parse(line);
      } catch {
        throw new SyntaxError('invalid NDJSON record');
      }
      if (!record || !Number.isSafeInteger(record.id) || typeof record.message !== 'string') {
        throw new Error('invalid record');
      }
      if (record.id !== nextId) throw new Error('unexpected record ID');
      nextId += 1;
      pending = '';
      start = lf + 1;
      yield record;
    }
  }

  if (pending !== '') throw new Error('incomplete NDJSON line');
}

export function ndjsonRecords(chunks) {
  const iterator = parseRecords(chunks);
  let cancelled = false;

  return new ReadableStream({
    async pull(controller) {
      const { value, done } = await iterator.next();
      if (cancelled) return;
      if (done) controller.close();
      else controller.enqueue(value);
    },
    async cancel() {
      cancelled = true;
      await iterator.return();
    },
  }, { highWaterMark: 0 });
}
