const LOCAL_OPERATOR_API_ORIGIN = "http://127.0.0.1:8787";

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

  const hostname = String(window.location?.hostname || "").toLowerCase();
  const port = String(window.location?.port || "");
  const isLocalHost = hostname === "127.0.0.1" || hostname === "localhost";
  if (isLocalHost && port && port !== "8787") {
    return LOCAL_OPERATOR_API_ORIGIN;
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

async function requestJson(path, options = {}) {
  const requestUrl = buildRequestUrl(path);
  let response;
  try {
    response = await fetch(requestUrl, options);
  } catch (networkError) {
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
    throw buildOperatorFetchError({
      requestPath: path,
      requestUrl,
      response,
      payload,
      parseError,
    });
  }
  return payload;
}

export async function fetchJson(path) {
  return requestJson(path, { cache: "no-store" });
}

export async function postJson(path, body) {
  return requestJson(path, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body || {}),
  });
}
