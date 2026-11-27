const utf8 = new TextDecoder('utf-8', {
  fatal: true,
  ignoreBOM: true,
});

function parseEvent(bytes, line) {
  if (bytes.length === 0) {
    throw new Error(`line ${line}: empty line`);
  }
  if (bytes.includes(0x0d)) throw new Error(`line ${line}: unexpected CR`);

  let text;
  try {
    text = utf8.decode(bytes);
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
      !Object.hasOwn(record, 'event') || typeof record.event !== 'string') {
    throw new Error(`line ${line}: expected an event string`);
  }
  return record;
}

async function* readEvents(source, maxLineBytes) {
  const pending = new Uint8Array(maxLineBytes + 1);
  let used = 0;
  let line = 1;

  for await (const chunk of source) {
    for (const byte of chunk) {
      if (byte === 0x0a) {
        const endsInCr = pending[used - 1] === 0x0d;
        const end = endsInCr ? used - 1 : used;
        const bytes = pending.subarray(0, end);
        const record = parseEvent(bytes, line);
        used = 0;
        line += 1;
        yield record;
        continue;
      }

      if (used === pending.length ||
          (used === maxLineBytes && byte !== 0x0d)) {
        throw new Error(`line ${line}: exceeds ${maxLineBytes} bytes`);
      }
      pending[used] = byte;
      used += 1;
    }
  }

  if (used !== 0) {
    throw new Error(`line ${line}: missing final LF`);
  }
}

const chunks = [
  Buffer.from('{"event":"ø"}', 'utf8').subarray(0, 11),
  Buffer.from([0xb8, 0x22, 0x7d, 0x0d]),
  Buffer.from('\n{"event":"café"}\n'),
];

for await (const record of readEvents(chunks, 32)) {
  console.log(record.event);
}
