import assert from 'node:assert/strict';

const accepted = new Map([
  ['run-1', '10:55'],
  ['run-2', '10:56'],
  ['run-3', '10:57'],
  ['run-4', '10:58'],
]);
const deliveries = [
  { runId: 'run-1', status: 'ok', finishedAt: '10:56', receivedAt: '10:56' },
  { runId: 'run-3', status: 'error', finishedAt: '10:58', receivedAt: '10:58' },
  { runId: 'run-1', status: 'ok', finishedAt: '10:56', receivedAt: '10:59' },
  { runId: 'run-2', status: 'ok', finishedAt: '10:57', receivedAt: '11:06' },
];
const deadlineMinutes = 2;

// All timestamps use the same date and time zone, with zero-padded hours.
function minutesOfDay(time) {
  const [hours, minutes] = time.split(':').map(Number);
  return hours * 60 + minutes;
}

function summarize(asOf) {
  const outcomes = new Map();
  for (const { runId, status, finishedAt, receivedAt } of deliveries) {
    if (!accepted.has(runId) || receivedAt > asOf) continue;
    const outcome = { status, finishedAt };
    if (outcomes.has(runId)) {
      assert.deepEqual(outcomes.get(runId), outcome, `conflicting outcome for ${runId}`);
    }
    outcomes.set(runId, outcome);
  }

  let met = 0;
  for (const [runId, { status, finishedAt }] of outcomes) {
    const elapsed = minutesOfDay(finishedAt) - minutesOfDay(accepted.get(runId));
    if (status === 'ok' && elapsed <= deadlineMinutes) met += 1;
  }

  return {
    total: accepted.size,
    met,
    missed: outcomes.size - met,
    unknown: accepted.size - outcomes.size,
  };
}

for (const asOf of ['11:02', '11:08']) {
  const { total, met, missed, unknown } = summarize(asOf);
  const minimum = 100 * met / total;
  const maximum = 100 * (met + unknown) / total;
  console.log(`asOf=${asOf} total=${total} met=${met} missed=${missed} unknown=${unknown}`);
  console.log(`  range=${minimum}%..${maximum}%`);
}
