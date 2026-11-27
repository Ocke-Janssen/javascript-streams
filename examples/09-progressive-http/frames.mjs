const maxFrameUnits = 4096;

function checkLength(units) {
  if (units > maxFrameUnits) throw new Error(`frame exceeds ${maxFrameUnits} UTF-16 code units`);
}

export async function readDemoFrames(body, separator, onFrame) {
  let pending = '';
  for await (const text of body.pipeThrough(new TextDecoderStream('utf-8', { fatal: true }))) {
    pending += text;
    let end;
    while ((end = pending.indexOf(separator)) !== -1) {
      checkLength(end);
      onFrame(pending.slice(0, end));
      pending = pending.slice(end + separator.length);
    }
    // With the SSE separator, the last LF may be the start of the delimiter.
    const delimiterPrefix = separator === '\n\n' && pending.endsWith('\n') ? 1 : 0;
    checkLength(pending.length - delimiterPrefix);
  }
  if (pending !== '') throw new Error('incomplete frame');
}

// The measured routes use LF and a space after the colon in each field.
export function parseDemoSse(frame) {
  const lines = frame.split('\n');
  const data = lines.filter((line) => line.startsWith('data: ')).map((line) => line.slice(6));
  if (data.length === 0) return undefined;
  return {
    type: lines.find((line) => line.startsWith('event: '))?.slice(7) ?? 'message',
    id: lines.find((line) => line.startsWith('id: '))?.slice(4),
    data: data.join('\n'),
  };
}
