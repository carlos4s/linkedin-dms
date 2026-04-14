// Desearch LinkedIn DMs — Chrome Extension Background Service Worker
// Monitors li_at cookie changes and captures x-li-track / csrf-token headers.

const LINKEDIN_DOMAIN = "linkedin.com";
const VOYAGER_API_PATTERN = "https://www.linkedin.com/voyager/api/*";

const SERVICE_URL_DEFAULT = "http://localhost:8899";

// ─── Helpers ─────────────────────────────────────────────────────────────────

async function getConfig() {
  const result = await chrome.storage.local.get({
    serviceUrl: SERVICE_URL_DEFAULT,
    accountId: null,
    xLiTrack: null,
    csrfToken: null,
    conversationsQueryId: null,
    messagesQueryId: null,
  });
  return result;
}

async function setStatus(status, error = null) {
  await chrome.storage.local.set({
    lastStatus: status,
    lastError: error,
    lastUpdated: new Date().toISOString(),
  });
}

async function getLinkedInCookies() {
  const cookies = {};
  const liAt = await chrome.cookies.get({
    url: "https://www.linkedin.com",
    name: "li_at",
  });
  if (liAt) cookies.li_at = liAt.value;

  const jsessionid = await chrome.cookies.get({
    url: "https://www.linkedin.com",
    name: "JSESSIONID",
  });
  if (jsessionid) cookies.JSESSIONID = jsessionid.value.replace(/"/g, "");

  return cookies;
}

function buildRuntimeHints(state) {
  const hints = {};
  if (state.xLiTrack) hints.x_li_track = state.xLiTrack;
  if (state.csrfToken) hints.csrf_token = state.csrfToken;
  if (state.conversationsQueryId) hints.conversations_query_id = state.conversationsQueryId;
  if (state.messagesQueryId) hints.messages_query_id = state.messagesQueryId;
  return Object.keys(hints).length > 0 ? hints : null;
}

function extractGraphqlQueryId(url) {
  if (!url) return null;
  try {
    const parsed = new URL(url);
    const queryId = parsed.searchParams.get("queryId");
    if (!queryId) return null;
    if (queryId.startsWith("messengerConversations.")) {
      return { storageKey: "conversationsQueryId", value: queryId };
    }
    if (queryId.startsWith("messengerMessages.")) {
      return { storageKey: "messagesQueryId", value: queryId };
    }
  } catch (_err) {
    return null;
  }
  return null;
}

// ─── Cookie Monitoring ──────────────────────────────────────────────────────

chrome.cookies.onChanged.addListener(({ cookie, removed }) => {
  if (cookie.domain.includes("linkedin.com") && cookie.name === "li_at" && !removed) {
    // Get JSESSIONID too
    chrome.cookies.get({ url: "https://www.linkedin.com", name: "JSESSIONID" }, async (jsession) => {
      try {
        const config = await getConfig();
        const cookies = {
          li_at: cookie.value,
          JSESSIONID: jsession?.value?.replace(/"/g, "") || null,
        };

        if (config.accountId) {
          await pushRefresh(config, cookies);
        } else {
          await registerAccount(config, cookies);
        }
      } catch (err) {
        console.error("[desearch] cookie change handler error:", err);
        await setStatus("error", err.message);
      }
    });
  }
});

async function pushRefresh(config, cookies) {
  const payload = {
    account_id: config.accountId,
    li_at: cookies.li_at,
    jsessionid: cookies.JSESSIONID || null,
    runtime_hints: buildRuntimeHints(config),
  };

  const resp = await fetch(`${config.serviceUrl}/accounts/refresh`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload),
  });

  if (!resp.ok) {
    const detail = await resp.text();
    throw new Error(`Refresh failed (${resp.status}): ${detail}`);
  }

  console.log("[desearch] cookie refresh pushed successfully");
  await setStatus("connected");
}

async function registerAccount(config, cookies) {
  const payload = {
    label: "chrome-extension",
    li_at: cookies.li_at,
    jsessionid: cookies.JSESSIONID || null,
    runtime_hints: buildRuntimeHints(config),
  };

  const resp = await fetch(`${config.serviceUrl}/accounts`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload),
  });

  if (!resp.ok) {
    const detail = await resp.text();
    throw new Error(`Account registration failed (${resp.status}): ${detail}`);
  }

  const data = await resp.json();
  await chrome.storage.local.set({ accountId: data.account_id });
  console.log("[desearch] account registered:", data.account_id);
  await setStatus("connected");
}

// ─── Header Capture ─────────────────────────────────────────────────────────
// Intercept outgoing LinkedIn Voyager API requests to capture x-li-track and
// csrf-token header values from the real browser session.

chrome.webRequest.onSendHeaders.addListener(
  (details) => {
    const requestHeaders = details.requestHeaders || [];
    const track = requestHeaders.find(h => h.name === "x-li-track");
    const csrf = requestHeaders.find(h => h.name === "csrf-token");
    const queryId = extractGraphqlQueryId(details.url);
    if (track || csrf || queryId) {
      const updates = {};
      if (track) updates.xLiTrack = track.value;
      if (csrf) updates.csrfToken = csrf.value;
      if (queryId) updates[queryId.storageKey] = queryId.value;
      // store for provider use
      chrome.storage.local.set(updates);
    }
  },
  { urls: [VOYAGER_API_PATTERN] },
  ["requestHeaders"]
);

// ─── Message handling (from popup) ──────────────────────────────────────────

chrome.runtime.onMessage.addListener((msg, _sender, sendResponse) => {
  if (msg.type === "MANUAL_SYNC") {
    handleManualSync()
      .then((result) => sendResponse({ ok: true, data: result }))
      .catch((err) => sendResponse({ ok: false, error: err.message }));
    return true; // keep channel open for async response
  }

  if (msg.type === "MANUAL_REFRESH") {
    handleManualRefresh()
      .then(() => sendResponse({ ok: true }))
      .catch((err) => sendResponse({ ok: false, error: err.message }));
    return true;
  }
});

async function handleManualSync() {
  const config = await getConfig();
  if (!config.accountId) {
    throw new Error("No account registered. Log in to LinkedIn first.");
  }

  const resp = await fetch(`${config.serviceUrl}/sync`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      account_id: config.accountId,
      runtime_hints: buildRuntimeHints(config),
    }),
  });

  if (!resp.ok) {
    const detail = await resp.text();
    throw new Error(`Sync failed (${resp.status}): ${detail}`);
  }

  const data = await resp.json();
  await setStatus("connected");
  return data;
}

async function handleManualRefresh() {
  const config = await getConfig();
  const cookies = await getLinkedInCookies();

  if (!cookies.li_at) {
    throw new Error("Not logged in to LinkedIn — no li_at cookie found.");
  }

  if (config.accountId) {
    await pushRefresh(config, cookies);
  } else {
    await registerAccount(config, cookies);
  }
}
