export async function fetchJson(path) {
  const response = await fetch(path, { cache: "no-store" });
  const payload = await response.json();
  if (!response.ok) {
    const error = new Error(payload.message || payload.result || `HTTP ${response.status}`);
    error.payload = payload;
    throw error;
  }
  return payload;
}

export async function postJson(path, body) {
  const response = await fetch(path, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body || {}),
  });
  const payload = await response.json();
  if (!response.ok) {
    const error = new Error(payload.message || payload.result || `HTTP ${response.status}`);
    error.payload = payload;
    throw error;
  }
  return payload;
}
