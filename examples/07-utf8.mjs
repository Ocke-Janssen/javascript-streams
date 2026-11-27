const bytes = Buffer.from('Grüße\n', 'utf8');
const chunks = [bytes.subarray(0, 3)];
if (!process.argv.includes('--incomplete')) chunks.push(bytes.subarray(3));
const decoder = new TextDecoder('utf-8', { fatal: true });

for (const chunk of chunks) {
  const text = decoder.decode(chunk, { stream: true });
  console.log(JSON.stringify(text));
}
console.log(`flush=${JSON.stringify(decoder.decode())}`);
