import assert from 'node:assert/strict';
import { spawnSync } from 'node:child_process';
import { mkdtemp, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { fileURLToPath } from 'node:url';

const directory = await mkdtemp(join(tmpdir(), 'streams-ndjson-'));
const example = fileURLToPath(new URL('./07-ndjson.mjs', import.meta.url));
const input = join(directory, 'input.ndjson');
const exact = '{"event":"ø"}';
const prefix = '{"event":"ok"}\n';

async function check(name, bytes, limit, expected) {
  await writeFile(input, bytes);
  for (const width of [1, 7, 64]) {
    const result = spawnSync(process.execPath, [example, input, `${width}`, `${limit}`], {
      encoding: 'utf8',
      timeout: 5000,
    });
    if (result.error) throw result.error;
    assert.deepEqual(
      [result.status, result.stdout, result.stderr],
      expected,
      `${name}, read width ${width}`,
    );
  }
  console.log(`${name}: same result at 1, 7, and 64 bytes`);
}

try {
  await check('CRLF does not count toward the limit', exact + '\r\n',
    Buffer.byteLength(exact), [0, exact + '\n', '']);
  await check('overlong line', exact + '\r\n',
    Buffer.byteLength(exact) - 1, [1, '', 'line 1: exceeds 13 bytes\n']);
  await check('valid JSON at EOF without LF', prefix + exact,
    64, [0, prefix + exact + '\n', '']);
  await check('missing closing brace at EOF', prefix + exact.slice(0, -1),
    64, [1, prefix, 'line 2: invalid JSON\n']);
  await check('CR at EOF is not a delimiter', exact + '\r',
    64, [1, '', 'line 1: unexpected CR\n']);
  await check('earlier JSON error, later UTF-8 error',
    Buffer.concat([Buffer.from(prefix + '{\n'), Buffer.from([0xff])]),
    64, [1, prefix, 'line 2: invalid JSON\n']);
  await check('valid JSON, invalid schema', '{"event":"ok","id":9007199254740993}',
    64, [1, '', 'line 1: expected a string id\n']);
  await check('valid string ID', '{"event":"ok","id":"9007199254740993"}',
    64, [0, '{"event":"ok","id":"9007199254740993"}\n', '']);
  const duplicateWithStringLast = '{"event":"ok","id":8,"id":"8"}';
  await check('duplicate ID with a string last', duplicateWithStringLast,
    64, [0, duplicateWithStringLast + '\n', '']);
  await check('duplicate ID with a number last', '{"event":"ok","id":"8","id":8}',
    64, [1, '', 'line 1: expected a string id\n']);
  await check('invalid UTF-8 at end of input',
    Buffer.concat([Buffer.from(prefix), Buffer.from([0xc3])]),
    64, [1, prefix, 'line 2: invalid UTF-8\n']);
} finally {
  await rm(directory, { recursive: true, force: true });
}
