function normalizeBaseUrl(value) {
  const trimmed = String(value || "").trim();
  if (!trimmed) {
    return "";
  }
  return trimmed.replace(/\/+$/, "");
}

function resolveApiBaseUrl() {
  if (typeof window === "undefined") {
    return "";
  }

  const explicitBase = normalizeBaseUrl(window.__AEGIS_API_BASE_URL);
  if (explicitBase) {
    return explicitBase;
  }

  return "";
}

function buildRequestUrl(path) {
  const rawPath = String(path || "").trim();
  if (/^https?:\/\//i.test(rawPath)) {
    return rawPath;
  }
  const normalizedPath = rawPath.startsWith("/") ? rawPath : `/${rawPath}`;
  const baseUrl = resolveApiBaseUrl();
  return baseUrl ? `${baseUrl}${normalizedPath}` : normalizedPath;
}

function isCrossOriginRequest(requestUrl) {
  if (typeof window === "undefined") {
    return false;
  }
  try {
    const target = new URL(requestUrl, window.location.origin);
    return target.origin !== window.location.origin;
  } catch {
    return false;
  }
}

function failureClassFor({ response, parseError, networkError, requestUrl }) {
  if (networkError) {
    return isCrossOriginRequest(requestUrl) ? "CORS_OR_BASE_URL_ISSUE" : "BACKEND_UNREACHABLE";
  }
  if (response && response.status === 404) {
    return "ROUTE_MISSING";
  }
  if (response && response.status >= 500) {
    return "BACKEND_EXCEPTION";
  }
  if (parseError) {
    return "PAYLOAD_INVALID";
  }
  if (response && !response.ok) {
    return "HTTP_ERROR";
  }
  return "UNKNOWN";
}

function nextActionForFailure(failureClass, endpointAttempted) {
  if (failureClass === "CORS_OR_BASE_URL_ISSUE") {
    return `Verify local API base/CORS and confirm ${endpointAttempted} is reachable from the current origin.`;
  }
  if (failureClass === "BACKEND_UNREACHABLE") {
    return `Start or restart the operator backend and verify listener availability for ${endpointAttempted}.`;
  }
  if (failureClass === "ROUTE_MISSING") {
    return "Confirm the backend route exists and that the running service version includes this endpoint.";
  }
  if (failureClass === "BACKEND_EXCEPTION") {
    return "Inspect backend logs for the endpoint exception and preserve fail-closed response semantics.";
  }
  if (failureClass === "PAYLOAD_INVALID") {
    return "Validate response status/content-type/body against the API contract; payload must be valid JSON.";
  }
  return "Inspect browser network + backend logs to isolate transport vs endpoint contract failure.";
}

function emitConnectionState(state, detail = {}) {
  if (typeof window === "undefined") {
    return;
  }
  const payload = {
    state,
    last_checked_at: new Date().toISOString(),
    ...detail,
  };
  window.__AEGIS_CONNECTION_STATE = payload;
  try {
    window.dispatchEvent(new CustomEvent("aegis:connection-state", { detail: payload }));
  } catch {
    window.dispatchEvent(new Event("aegis:connection-state"));
  }
}

function buildOperatorFetchError({
  requestPath,
  requestUrl,
  response,
  payload,
  parseError,
  networkError,
}) {
  const failureClass = failureClassFor({ response, parseError, networkError, requestUrl });
  const statusCode = response ? response.status : null;
  const payloadMessage = payload && typeof payload === "object" ? payload.message || payload.result : "";
  const message = payloadMessage || `${failureClass}: ${requestPath}`;
  const error = new Error(message);
  error.name = "OperatorFetchError";
  error.payload = payload;
  error.operatorSafe = {
    endpointAttempted: requestUrl,
    failureClass,
    backendUnreachable: failureClass === "BACKEND_UNREACHABLE" || failureClass === "CORS_OR_BASE_URL_ISSUE",
    routeMissing: failureClass === "ROUTE_MISSING",
    payloadInvalid: failureClass === "PAYLOAD_INVALID",
    statusCode,
    nextAction: nextActionForFailure(failureClass, requestUrl),
  };
  return error;
}

const inFlightJsonRequests = new Map();

function cacheableGetKey(path, options = {}) {
  const method = String(options.method || "GET").toUpperCase();
  if (method !== "GET") {
    return "";
  }
  return buildRequestUrl(path);
}


async function requestJson(path, options = {}) {
  const requestUrl = buildRequestUrl(path);
  const startedAt = typeof performance !== "undefined" ? performance.now() : Date.now();
  let response;
  try {
    response = await fetch(requestUrl, options);
  } catch (networkError) {
    emitConnectionState("BACKEND_UNAVAILABLE", {
      endpoint: requestUrl,
      recovery_command: "npm run aegis:ui:restart",
    });
    throw buildOperatorFetchError({
      requestPath: path,
      requestUrl,
      networkError,
    });
  }

  const rawText = await response.text();
  let payload = null;
  let parseError = null;
  if (rawText) {
    try {
      payload = JSON.parse(rawText);
    } catch (err) {
      parseError = err;
    }
  }

  if (!response.ok || parseError || payload === null || typeof payload !== "object") {
    emitConnectionState(response && response.status >= 500 ? "DISCONNECTED" : "RECONNECTING", {
      endpoint: requestUrl,
      recovery_command: "npm run aegis:ui:restart",
    });
    throw buildOperatorFetchError({
      requestPath: path,
      requestUrl,
      response,
      payload,
      parseError,
    });
  }
  const durationMs = Math.round(((typeof performance !== "undefined" ? performance.now() : Date.now()) - startedAt) * 10) / 10;
  console.info("[aegis-api-timing]", {
    endpoint: path,
    request_url: requestUrl,
    status: response.status,
    duration_ms: durationMs,
  });
  emitConnectionState("CONNECTED", { endpoint: requestUrl });
  return payload;
}

export async function fetchJson(path) {
  const options = { cache: "no-store" };
  const requestKey = cacheableGetKey(path, options);
  if (requestKey && inFlightJsonRequests.has(requestKey)) {
    if (typeof window !== "undefined") {
      window.__AEGIS_DUPLICATE_FETCH_COUNT = Number(window.__AEGIS_DUPLICATE_FETCH_COUNT || 0) + 1;
    }
    return inFlightJsonRequests.get(requestKey);
  }
  const request = requestJson(path, options);
  if (requestKey) {
    inFlightJsonRequests.set(requestKey, request);
    request.finally(() => {
      inFlightJsonRequests.delete(requestKey);
    });
  }
  return request;
}

export async function postJson(path, body) {
  return requestJson(path, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body || {}),
  });
}

export async function patchJson(path, body) {
  return requestJson(path, {
    method: "PATCH",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body || {}),
  });
}
