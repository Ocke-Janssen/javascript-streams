import { readFile } from 'node:fs/promises';
import { summarizeRunLog } from './14-run-log.mjs';

const paths = process.argv.slice(2);
if (paths.length === 0) {
  console.error('Usage: node examples/14-summarize.mjs <log.ndjson> [...]');
  process.exitCode = 1;
} else {
  try {
    const summaries = new Map();
    for (const path of paths) {
      const summary = summarizeRunLog(await readFile(path), path);
      if (summaries.has(summary.runId)) {
        throw new Error(`${path}: duplicate run ID in the input files`);
      }
      summaries.set(summary.runId, summary);
    }
    for (const summary of summaries.values()) {
      console.log(JSON.stringify(summary));
    }
  } catch (error) {
    console.error(error.message);
    process.exitCode = 1;
  }
}
