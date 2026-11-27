import { createReadStream } from 'node:fs';
import { pipeline } from 'node:stream/promises';

const decoder = new TextDecoder('utf-8', { fatal: true, ignoreBOM: true });

function validateLine(bytes, line, terminated) {
  if (terminated && bytes.at(-1) === 0x0d) bytes = bytes.subarray(0, -1);
  if (bytes.length === 0) return undefined;
  if (bytes.includes(0x0d)) throw new Error(`line ${line}: unexpected CR`);

  let text;
  try {
    text = decoder.decode(bytes);
  } catch (cause) {
    throw new Error(`line ${line}: invalid UTF-8`, { cause });
  }

  let record;
  try {
    record = JSON.parse(text);
  } catch (cause) {
    throw new Error(`line ${line}: invalid JSON`, { cause });
  }
  if (record === null || typeof record !== 'object' ||
      Array.isArray(record) || typeof record.event !== 'string') {
    throw new Error(`line ${line}: expected an object with an event string`);
  }
  if (Object.hasOwn(record, 'id') && typeof record.id !== 'string') {
    throw new Error(`line ${line}: expected a string id`);
  }
  if (Object.hasOwn(record, 'count') &&
      (!Number.isSafeInteger(record.count) || record.count < 0)) {
    throw new Error(`line ${line}: expected a non-negative safe integer count`);
  }
  return text;
}

async function* readNdjson(source, maxLineBytes) {
  // A single extra byte may be the CR of a CRLF delimiter.
  const pending = Buffer.alloc(maxLineBytes + 1);
  let used = 0;
  let line = 1;
  const tooLong = () => new Error(`line ${line}: exceeds ${maxLineBytes} bytes`);

  for await (const chunk of source) {
    let offset = 0;
    while (offset < chunk.length) {
      const lf = chunk.indexOf(0x0a, offset);
      const end = lf === -1 ? chunk.length : lf;
      const size = end - offset;
      const length = used + size;
      const last = size > 0 ? chunk[end - 1] : pending[used - 1];
      if (length > maxLineBytes && (length > pending.length || last !== 0x0d)) throw tooLong();

      chunk.copy(pending, used, offset, end);
      used += size;
      if (lf === -1) break;

      const text = validateLine(pending.subarray(0, used), line, true);
      line += 1;
      used = 0;
      offset = lf + 1;
      if (text !== undefined) yield text + '\n';
    }
  }

  if (used > 0) {
    if (used > maxLineBytes) throw tooLong();
    const text = validateLine(pending.subarray(0, used), line, false);
    if (text !== undefined) yield text + '\n';
  }
}

const [input = 'fixtures/07-events.ndjson', chunkArg = '7', limitArg = '256'] = process.argv.slice(2);
const chunkBytes = Number(chunkArg);
const maxLineBytes = Number(limitArg);
if (!Number.isInteger(chunkBytes) || chunkBytes < 1 || chunkBytes > 65_536 ||
    !Number.isInteger(maxLineBytes) || maxLineBytes < 1 || maxLineBytes > 1_048_576) {
  console.error('Usage: node examples/07-ndjson.mjs [input] [chunk-bytes: 1–65536] [max-line-bytes: 1–1048576]');
  process.exit(1);
}

try {
  await pipeline(
    createReadStream(input, { highWaterMark: chunkBytes }),
    (source) => readNdjson(source, maxLineBytes),
    process.stdout,
  );
} catch (error) {
  console.error(error.message);
  process.exitCode = 1;
}
