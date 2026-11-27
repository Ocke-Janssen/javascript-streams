const sample = Uint8Array.of(0, 65, 255, 10, 0);

async function attempt(body, extra = {}) {
  try {
    const response = await fetch('/api/upload', {
      method: 'POST',
      body,
      headers: { 'Content-Type': 'application/octet-stream' },
      signal: AbortSignal.timeout(3000),
      ...extra,
    });
    if (!response.ok) {
      await response.body?.cancel().catch(() => {});
      return { status: response.status, match: false };
    }
    return { status: response.status, ...(await response.json()) };
  } catch (error) {
    return { error: error.name, match: false };
  }
}

export async function probeUpload() {
  const blob = await attempt(new Blob([sample]));
  const stream = await attempt(new ReadableStream({
    start(controller) {
      controller.enqueue(sample.subarray(0, 2));
      controller.enqueue(sample.subarray(2));
      controller.close();
    },
  }), { duplex: 'half' });

  return { blob, stream };
}
