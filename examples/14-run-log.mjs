const endReasons = new Map([
  ['ok', new Set(['completed'])],
  ['error', new Set(['invalid_json', 'unexpected'])],
  ['aborted', new Set(['client_disconnect'])],
]);
const modes = new Set(['ok', 'slow-source', 'slow-sink', 'bad-record', 'abort']);

function isCount(value) {
  return Number.isSafeInteger(value) && value >= 0;
}

function isName(value) {
  return typeof value === 'string' && value.trim() !== '';
}

function parseEntries(bytes, label) {
  let text;
  try {
    text = new TextDecoder('utf-8', { fatal: true }).decode(bytes);
  } catch {
    throw new Error(`${label}: invalid UTF-8 in log`);
  }

  return text.trimEnd().split('\n').map((line, index) => {
    try {
      return JSON.parse(line);
    } catch {
      throw new Error(`${label}: invalid JSON on line ${index + 1}`);
    }
  });
}

function validateEnd(end, label) {
  const {
    status, reason, elapsedMs, readBytes, recordsRead, processed, dropped, rejected,
    peakSinkPendingRecords, sourceWaitMs, sinkWriteMs,
  } = end;
  if (!endReasons.get(status)?.has(reason)) {
    throw new Error(`${label}: invalid status or reason in run.end`);
  }
  const counts = [
    readBytes, recordsRead, processed, dropped, rejected,
    peakSinkPendingRecords, sourceWaitMs, sinkWriteMs,
  ];
  if (!counts.every(isCount)) {
    throw new Error(`${label}: missing or invalid measurements in run.end`);
  }

  const classified = processed + dropped + rejected;
  if (classified > recordsRead || readBytes < recordsRead ||
      (recordsRead === 0 && readBytes !== 0) ||
      (status === 'ok' && (classified !== recordsRead || rejected !== 0)) ||
      (reason === 'invalid_json' && rejected !== 1) ||
      peakSinkPendingRecords > recordsRead - dropped - rejected ||
      sourceWaitMs > elapsedMs || sinkWriteMs > elapsedMs) {
    throw new Error(`${label}: inconsistent measurements in run.end`);
  }
}

function agreesWithEnd(entry, end) {
  switch (entry.event) {
    case 'source.first':
      return isCount(entry.bytes) && entry.bytes > 0 && entry.bytes <= end.readBytes;
    case 'sink.first':
      return entry.processed === 1 && end.processed >= 1;
    case 'sink.queue':
      return isCount(entry.pendingRecords) && entry.pendingRecords > 0 &&
        entry.pendingRecords <= end.peakSinkPendingRecords;
    case 'record.rejected':
      return entry.stage === 'decode' && entry.reason === 'invalid_json' &&
        isCount(entry.record) && entry.record > 0 && entry.record <= end.recordsRead &&
        end.rejected > 0;
    default:
      return true;
  }
}

export function summarizeRunLog(bytes, label) {
  const entries = parseEntries(bytes, label);
  const start = entries[0];
  const end = entries.at(-1);
  if (start?.event !== 'run.start' || end?.event !== 'run.end' || !isName(start.runId) ||
      entries.some((entry, index) => !isName(entry?.event) || entry.runId !== start.runId ||
        !isCount(entry.elapsedMs) || (index > 0 && entry.elapsedMs < entries[index - 1].elapsedMs) ||
        (entry.event === 'run.start' && index !== 0) ||
        (entry.event === 'run.end' && index !== entries.length - 1))) {
    throw new Error(`${label}: expected one complete, ordered run with a shared ID`);
  }
  if (start.mode !== undefined && !modes.has(start.mode)) {
    throw new Error(`${label}: invalid mode in run.start`);
  }

  validateEnd(end, label);
  if (!entries.every(entry => agreesWithEnd(entry, end))) {
    throw new Error(`${label}: an event contradicts run.end`);
  }

  const { status, reason, elapsedMs, readBytes, recordsRead, processed, dropped, rejected,
    peakSinkPendingRecords, sourceWaitMs, sinkWriteMs } = end;
  return {
    runId: start.runId, mode: start.mode, status, reason, elapsedMs, readBytes, recordsRead,
    processed, dropped, rejected, peakSinkPendingRecords, sourceWaitMs, sinkWriteMs,
  };
}
