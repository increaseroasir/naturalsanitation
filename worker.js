/**
 * Cloudflare Worker
 *
 * Routes:
 *   POST /create-payment-intent — Stripe PaymentIntent (env STRIPE_SECRET_KEY)
 *   POST /confirm-purchase      — Hyros order creation after successful Stripe payment (env HYROS_API_KEY)
 *   POST /jobber-sale           — Jobber invoice → Hyros order with GHL tag lookup (env GHL_API_TOKEN, GHL_LOCATION_ID, HYROS_API_KEY)
 *   POST /stripe-webhook        — Stripe webhook for subscription renewals → Hyros recurring order (env STRIPE_WEBHOOK_SECRET, HYROS_API_KEY)
 *   POST /meta-capi — Meta Conversions API (env META_CAPI_ACCESS_TOKEN, never expose to browser)
 *   POST /ghl-lead — GoHighLevel Contacts API upsert (env GHL_API_TOKEN, GHL_LOCATION_ID; never in browser)
 *   POST /client-observe — lightweight browser/lead observability sink for debugging failed lead delivery
 *   POST /lead-receipt — Worker-side backup logging of submitted lead details for recovery outside GoHighLevel
 *
 * Secrets / vars (Cloudflare dashboard → Worker → Settings → Variables):
 *   STRIPE_SECRET_KEY           — required for PaymentIntents
 *   HYROS_API_KEY               — required for /confirm-purchase (Hyros order creation)
 *   META_CAPI_ACCESS_TOKEN      — required for /meta-capi (never in the browser)
 *   GHL_API_TOKEN               — required for /ghl-lead (private integration / sub-account token)
 *   GHL_LOCATION_ID             — required for /ghl-lead (sub-account location id)
 *   GHL_FUNNEL_EVENT_FIELD_ID   — optional custom field id (Contacts) to store raw funnel event_id / jobber id
 *   META_TEST_EVENT_CODE        — optional, e.g. TEST28089 → Meta Graph `test_event_code` (Test Events only)
 *   GOOGLE_SHEET_WEBHOOK_URL    — optional Apps Script web app URL used by /lead-receipt for durable Google Sheets backup
 *   JOBBER_WEBHOOK_SECRET       — optional shared secret for /jobber-sale (set same value in Zapier webhook header)
 *   STRIPE_WEBHOOK_SECRET       — required for /stripe-webhook Stripe signature verification (whsec_...)
 *
 * Optional: browser may send `test_event_code` in the JSON body; the Worker forwards it only if it
 * matches /^TEST[A-Z0-9]+$/i (same format as Events Manager test codes).
 */

function corsHeaders() {
  return {
    'Access-Control-Allow-Origin': '*',
    'Access-Control-Allow-Methods': 'GET, POST, OPTIONS',
    'Access-Control-Allow-Headers': 'Content-Type, Idempotency-Key, X-Dashboard-Key, Cache-Control',
    'Access-Control-Max-Age': '86400',
  };
}

function json(body, status, headers) {
  return new Response(JSON.stringify(body), {
    status,
    headers: { 'Content-Type': 'application/json', ...headers },
  });
}

function isCreatePaymentIntentPath(pathname) {
  return pathname === '/create-payment-intent' || pathname.endsWith('/create-payment-intent');
}

function isMetaCapiPath(pathname) {
  return pathname === '/meta-capi' || pathname.endsWith('/meta-capi');
}

function isGhlLeadPath(pathname) {
  return pathname === '/ghl-lead' || pathname.endsWith('/ghl-lead');
}

function isClientObservePath(pathname) {
  return pathname === '/client-observe' || pathname.endsWith('/client-observe');
}

function isLeadReceiptPath(pathname) {
  return pathname === '/lead-receipt' || pathname.endsWith('/lead-receipt');
}

function isConfirmPurchasePath(pathname) {
  return pathname === '/confirm-purchase' || pathname.endsWith('/confirm-purchase');
}

function isJobberSalePath(pathname) {
  return pathname === '/jobber-sale' || pathname.endsWith('/jobber-sale');
}

function isStripeWebhookPath(pathname) {
  return pathname === '/stripe-webhook' || pathname.endsWith('/stripe-webhook');
}

const GHL_FORWARD_MAX_BYTES = 131072;
const GHL_API_BASE = 'https://services.leadconnectorhq.com';

function ghlAuthHeaders(token) {
  return {
    Authorization: 'Bearer ' + token,
    Version: '2021-07-28',
    'Content-Type': 'application/json',
    Accept: 'application/json',
  };
}

function ghlDigitsOnly(s) {
  return String(s || '').replace(/\D/g, '');
}

/** Returns E.164-style +1… when possible, or null if fewer than 10 digits. */
function ghlNormalizedPhone(body) {
  const d1 = ghlDigitsOnly(body.phone_e164);
  const d2 = ghlDigitsOnly(body.phone);
  const d = d1.length >= 10 ? d1 : d2;
  if (d.length < 10) return null;
  let n = d;
  if (n.length === 10) n = '1' + n;
  if (n.length === 11 && n[0] === '1') return '+' + n;
  return '+' + n;
}

function ghlBuildNameParts(body) {
  const full = String(body.full_name || body.name || '').trim();
  const firstPref = String(body.first_name || '').trim();
  if (full) {
    const bits = full.split(/\s+/).filter(Boolean);
    const fn = (firstPref || bits[0] || '').trim();
    const ln = bits.length > 1 ? bits.slice(1).join(' ') : '';
    return {
      firstName: fn || full.slice(0, 100),
      lastName: ln,
      name: full,
    };
  }
  if (firstPref) {
    return { firstName: firstPref, lastName: '', name: firstPref };
  }
  return { firstName: 'Customer', lastName: '', name: 'Customer' };
}

/** First non-empty funnel id from payload (browser + server). */
function ghlPrimaryEventId(body) {
  const a = String(body.event_id || '').trim();
  const b = String(body.jobber_event_id || '').trim();
  const raw = (a || b).replace(/\s+/g, ' ').slice(0, 200);
  return raw;
}

function ghlEventIdTag(raw) {
  if (!raw) return '';
  return 'event_id:' + raw;
}

/** Union-merge tags: preserve order (existing first), trim, dedupe case-insensitively (first spelling wins). */
function ghlUnionTags(existingList, incomingList) {
  const merged = [];
  const seenLower = new Set();
  function pushTag(raw) {
    const t = typeof raw === 'string' ? raw.trim() : '';
    if (!t) return;
    const k = t.toLowerCase();
    if (seenLower.has(k)) return;
    seenLower.add(k);
    merged.push(t);
  }
  if (Array.isArray(existingList)) {
    for (const t of existingList) {
      const s = typeof t === 'string' ? t.trim() : typeof t === 'object' && t && typeof t.name === 'string' ? t.name.trim() : '';
      if (s) pushTag(s);
    }
  }
  if (Array.isArray(incomingList)) {
    for (const t of incomingList) pushTag(t);
  }
  return merged;
}

function ghlNormalizeCustomFieldRow(f) {
  if (!f || typeof f !== 'object') return null;
  const id = typeof f.id === 'string' ? f.id : typeof f.field_id === 'string' ? f.field_id : '';
  if (!id) return null;
  const v =
    f.field_value != null
      ? String(f.field_value)
      : f.value != null
        ? String(f.value)
        : f.fieldValue != null
          ? String(f.fieldValue)
          : '';
  return { id, field_value: v.slice(0, 2000) };
}

function ghlCustomFieldsFromContact(contact) {
  const raw = contact && (contact.customFields || contact.customField);
  if (!Array.isArray(raw)) return [];
  const out = [];
  for (const f of raw) {
    const row = ghlNormalizeCustomFieldRow(f);
    if (row) out.push(row);
  }
  return out;
}

function ghlMergeCustomFieldArrays(existingRows, incomingRows, funnelFieldId, funnelRawEventId) {
  const byId = new Map();
  for (const row of existingRows || []) {
    if (row && row.id) byId.set(row.id, { id: row.id, field_value: row.field_value });
  }
  for (const row of incomingRows || []) {
    if (row && row.id) byId.set(row.id, { id: row.id, field_value: row.field_value });
  }
  const fid = funnelFieldId && String(funnelFieldId).trim();
  if (fid && funnelRawEventId) {
    byId.set(fid, { id: fid, field_value: String(funnelRawEventId).slice(0, 2000) });
  }
  return [...byId.values()].slice(0, 50);
}

function ghlExtractContactFromDuplicateJson(j) {
  if (!j || typeof j !== 'object') return null;
  if (j.contact && typeof j.contact === 'object') return j.contact;
  if (j.data && typeof j.data === 'object' && j.data.contact && typeof j.data.contact === 'object') {
    return j.data.contact;
  }
  if (Array.isArray(j.contacts) && j.contacts.length && typeof j.contacts[0] === 'object') return j.contacts[0];
  if (typeof j.id === 'string' && (Array.isArray(j.tags) || j.phone)) return j;
  return null;
}

/**
 * Load existing contact (same upsert key: phone/email) so we can union tags + custom fields before upsert.
 * Tries GET ?locationId&phone then POST JSON (API variants differ by account).
 */
async function ghlFetchDuplicateContact(token, locId, phone, email) {
  const loc = String(locId).trim();
  const em = email && typeof email === 'string' && email.includes('@') ? email.trim() : '';
  const tryGetUrl = new URL(GHL_API_BASE + '/contacts/search/duplicate');
  tryGetUrl.searchParams.set('locationId', loc);
  tryGetUrl.searchParams.set('phone', phone);
  if (em) tryGetUrl.searchParams.set('email', em);

  let res = await fetch(tryGetUrl.toString(), {
    method: 'GET',
    headers: { ...ghlAuthHeaders(token), Accept: 'application/json' },
  });
  if (!res.ok) {
    const postBody = { locationId: loc, phone };
    if (em) postBody.email = em;
    res = await fetch(GHL_API_BASE + '/contacts/search/duplicate', {
      method: 'POST',
      headers: ghlAuthHeaders(token),
      body: JSON.stringify(postBody),
    });
  }
  const text = await res.text();
  let j = {};
  try {
    j = text ? JSON.parse(text) : {};
  } catch {
    j = {};
  }
  if (!res.ok) {
    console.log('[ghl-lead] duplicate lookup not ok', res.status, (text || '').slice(0, 500));
    return null;
  }
  const c = ghlExtractContactFromDuplicateJson(j);
  if (!c) {
    console.log('[ghl-lead] duplicate lookup empty shape', JSON.stringify(Object.keys(j || {})).slice(0, 200));
  }
  return c;
}

/** Tags requested by this request (before union with CRM). */
function ghlBuildIncomingTagList(body) {
  const out = [];
  const st = String(body.status || '').toLowerCase();
  const isPaid = st === 'paid' || body.paid === true;

  // purchasefunnellead is added to ALL leads (every funnel touch)
  out.push('purchasefunnellead');

  // paid tag is ONLY added when Stripe checkout is completed
  if (isPaid) {
    out.push('paid');
  }
  if (Array.isArray(body.tags)) {
    for (const t of body.tags) {
      const s = typeof t === 'string' ? t.trim() : '';
      // Never allow 'paid' from frontend tags unless actually paid
      if (s && !(s === 'paid' && !isPaid)) out.push(s);
    }
  }
  if (st === 'lead_detail') {
    out.push('partial_lead', 'lead_gate', 'lead_detail', 'funnelv1optin');
  }
  if (st === 'partial_lead') {
    out.push('partial_lead');
  }
  if (isPaid) {
    out.push('partial_lead', 'initiate_checkout', 'purchased');
  }
  if (body.mark_initiate_checkout === true) {
    out.push('partial_lead', 'initiate_checkout');
  }
  const ev = ghlPrimaryEventId(body);
  const evTag = ghlEventIdTag(ev);
  if (evTag) out.push(evTag);
  return out;
}

function ghlBuildSource(body) {
  let base = String(body.source || 'ns-funnel').trim() || 'ns-funnel';
  if (body.status != null && body.status !== '') {
    base = (base + ' · status:' + String(body.status).slice(0, 64)).slice(0, 255);
  }
  const ev = body.event_id || body.jobber_event_id;
  if (ev) {
    base = (base + ' · event:' + String(ev).slice(0, 80)).slice(0, 255);
  }
  if (body.hyros_id) {
    base = (base + ' · hyros:' + String(body.hyros_id).slice(0, 40)).slice(0, 255);
  }
  const consentBits = [];
  for (const [k, v] of Object.entries(body)) {
    if (v === undefined || v === null || typeof v === 'object') continue;
    if (!/consent|tcpa|opt.?in|sms_legal|gdpr|legal/i.test(k)) continue;
    consentBits.push(k + '=' + String(v).slice(0, 48));
  }
  if (consentBits.length) {
    base = (base + ' · ' + consentBits.join('&')).slice(0, 255);
  }
  return base.slice(0, 255);
}

async function ghlCreateContactNote(token, contactId, rawEventId) {
  const cid = String(contactId || '').trim();
  const ev = String(rawEventId || '').trim();
  if (!cid || !ev) return { ok: false, skipped: true };
  const noteUrl = GHL_API_BASE + '/contacts/' + encodeURIComponent(cid) + '/notes';
  const noteBody = {
    title: 'Natural Sanitation Funnel Event ID',
    body: 'Raw funnel event ID: ' + ev,
    color: '#2563EB',
    pinned: false,
  };
  const res = await fetch(noteUrl, {
    method: 'POST',
    headers: ghlAuthHeaders(token),
    body: JSON.stringify(noteBody),
  });
  const text = await res.text();
  if (!res.ok) {
    console.warn('[ghl-lead] note create failed', res.status, (text || '').slice(0, 500));
    return { ok: false, status: res.status, body: (text || '').slice(0, 500) };
  }
  console.log('[ghl-lead] note create ok', JSON.stringify({ contactId: cid, eventId: ev }));
  return { ok: true };
}

function googleSheetWebhookUrl(env) {
  const auditUrl = env && typeof env.GOOGLE_SHEET_AUDIT_URL === 'string'
    ? String(env.GOOGLE_SHEET_AUDIT_URL || '').trim()
    : '';
  if (auditUrl && /^https:\/\//.test(auditUrl)) {
    return auditUrl;
  }
  const leadUrl = env && typeof env.GOOGLE_SHEET_WEBHOOK_URL === 'string'
    ? String(env.GOOGLE_SHEET_WEBHOOK_URL || '').trim()
    : '';
  if (leadUrl && /^https:\/\//.test(leadUrl)) {
    return leadUrl;
  }
  return '';
}

function parseObserveResponseJson(text) {
  if (!text || typeof text !== 'string') return null;
  try {
    return JSON.parse(text);
  } catch {
    return null;
  }
}

async function forwardMetaAuditToGoogleSheet(env, payload) {
  const url = googleSheetWebhookUrl(env);
  if (!url) {
    return { forwarded: false, reason: 'GOOGLE_SHEET_WEBHOOK_URL or GOOGLE_SHEET_AUDIT_URL not configured' };
  }
  const res = await fetch(url, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      Accept: 'application/json'
    },
    body: JSON.stringify(payload)
  });
  const text = await res.text();
  if (!res.ok) {
    throw new Error('Google Sheet webhook HTTP ' + res.status + ' ' + (text || '').slice(0, 300));
  }
  return {
    forwarded: true,
    status: res.status,
    body: (text || '').slice(0, 300)
  };
}

async function handleClientObserve(request, env) {
  try {
    const raw = await request.text();
    if (raw.length > 32768) {
      return json({ ok: false, error: 'Payload too large' }, 413, corsHeaders());
    }
    let parsed;
    try {
      parsed = raw ? JSON.parse(raw) : {};
    } catch {
      return json({ ok: false, error: 'Invalid JSON' }, 400, corsHeaders());
    }
    if (!parsed || typeof parsed !== 'object') {
      return json({ ok: false, error: 'Invalid body' }, 400, corsHeaders());
    }
    const observedIp =
      request.headers.get('CF-Connecting-IP') ||
      request.headers.get('True-Client-IP') ||
      (request.headers.get('X-Forwarded-For') || '').split(',')[0].trim() ||
      '';
    const requestUserAgent = request.headers.get('User-Agent') || '';
    const payload = {
      kind: String(parsed.kind || '').slice(0, 80),
      channel: String(parsed.channel || '').slice(0, 40),
      event_name: String(parsed.event_name || '').slice(0, 80),
      pixel_id: String(parsed.pixel_id || '').slice(0, 40),
      journey_event_id: String(parsed.journey_event_id || '').slice(0, 160),
      session_id: String(parsed.session_id || '').slice(0, 160),
      stage: String(parsed.stage || '').slice(0, 120),
      source: String(parsed.source || '').slice(0, 120),
      status: String(parsed.status || '').slice(0, 80),
      type: String(parsed.type || '').slice(0, 80),
      event_id: String(parsed.event_id || '').slice(0, 160),
      phone_last4: String(parsed.phone_last4 || '').slice(-4),
      service_zip: String(parsed.service_zip || '').slice(0, 20),
      currency: String(parsed.currency || '').slice(0, 16),
      custom_data_keys: String(parsed.custom_data_keys || '').slice(0, 500),
      pixel_track_type: String(parsed.pixel_track_type || '').slice(0, 40),
      capi_url: String(parsed.capi_url || '').slice(0, 200),
      page_url: String(parsed.page_url || '').slice(0, 500),
      referrer: String(parsed.referrer || '').slice(0, 500),
      browser_user_agent: String(parsed.user_agent || '').slice(0, 500),
      request_user_agent: String(requestUserAgent || '').slice(0, 500),
      observed_client_ip: String(observedIp || '').slice(0, 80),
      fbp_present: !!parsed.fbp_present,
      fbc_present: !!parsed.fbc_present,
      fbclid_present: !!parsed.fbclid_present,
      email_present: !!parsed.email_present,
      phone_present: !!parsed.phone_present,
      first_name_present: !!parsed.first_name_present,
      last_name_present: !!parsed.last_name_present,
      zip_present: !!parsed.zip_present,
      external_id_present: !!parsed.external_id_present,
      value_present: !!parsed.value_present,
      test_event_code_present: !!parsed.test_event_code_present,
      ok: typeof parsed.ok === 'boolean' ? parsed.ok : null,
      http_status: parsed.http_status || null,
      error_message: String(parsed.error_message || '').slice(0, 500),
      response_body: String(parsed.response_body || '').slice(0, 500),
      ts: parsed.ts || Date.now()
    };
    const responseJson = parseObserveResponseJson(payload.response_body);
    const auditPayload = {
      type: 'meta_event',
      timestamp: new Date(payload.ts || Date.now()).toISOString(),
      kind: payload.kind,
      channel: payload.channel,
      event_name: payload.event_name,
      event_id: payload.event_id,
      pixel_id: payload.pixel_id,
      journey_event_id: payload.journey_event_id,
      session_id: payload.session_id,
      page_url: payload.page_url,
      referrer: payload.referrer,
      client_ip: payload.observed_client_ip,
      user_agent: payload.browser_user_agent || payload.request_user_agent,
      browser_user_agent: payload.browser_user_agent,
      request_user_agent: payload.request_user_agent,
      stage: payload.stage,
      source: payload.source,
      status: payload.status,
      phone_last4: payload.phone_last4,
      service_zip: payload.service_zip,
      currency: payload.currency,
      custom_data_keys: payload.custom_data_keys,
      pixel_track_type: payload.pixel_track_type,
      capi_url: payload.capi_url,
      fbp_present: payload.fbp_present,
      fbc_present: payload.fbc_present,
      fbclid_present: payload.fbclid_present,
      email_present: payload.email_present,
      phone_present: payload.phone_present,
      first_name_present: payload.first_name_present,
      last_name_present: payload.last_name_present,
      zip_present: payload.zip_present,
      external_id_present: payload.external_id_present,
      value_present: payload.value_present,
      test_event_code_present: payload.test_event_code_present,
      ok: payload.ok,
      meta_response_status: payload.http_status,
      http_status: payload.http_status,
      fbtrace_id: responseJson && responseJson.fbtrace_id ? String(responseJson.fbtrace_id).slice(0, 120) : '',
      error_message: payload.error_message,
      response_body: payload.response_body
    };
    console.log('[client-observe]', JSON.stringify(payload));

    let sheetForward = { forwarded: false, reason: 'not_meta_event' };
    if (payload.kind === 'meta_event') {
      try {
        sheetForward = await forwardMetaAuditToGoogleSheet(env, auditPayload);
        console.log('[client-observe] google-sheet forward', JSON.stringify(sheetForward));
      } catch (sheetErr) {
        console.warn(
          '[client-observe] google-sheet forward failed',
          sheetErr && sheetErr.message ? String(sheetErr.message) : sheetErr
        );
        sheetForward = {
          forwarded: false,
          reason: sheetErr && sheetErr.message ? String(sheetErr.message) : 'forward_failed'
        };
      }
    }

    return json({ ok: true, sheetForward }, 200, corsHeaders());
  } catch (err) {
    console.warn('[client-observe] handler error', err && err.message ? String(err.message) : err);
    return json({ ok: false, error: 'Observability handler failed' }, 500, corsHeaders());
  }
}

/**
 * Map a Jobber invoice total (dollars) to a canonical product name for Hyros.
 * Covers all pricing tiers across main checkout, LTO, and quarterly pages.
 * Returns { name, sku } or null if no match.
 */
function jobberAmountToProduct(amountDollars) {
  const amt = Math.round(Number(amountDollars) * 100); // work in cents
  const table = [
    // Annual plans
    { cents: 19900, name: 'Annual Plan — 1 Bin',       sku: 'annual-1bin'       },
    { cents: 24900, name: 'Annual Plan — 2 Bins',      sku: 'annual-2bin'       },
    { cents: 25000, name: 'Annual Plan — 2 Bins',      sku: 'annual-2bin'       },
    { cents: 29900, name: 'Annual Plan — 3 Bins',      sku: 'annual-3bin'       },
    { cents: 34900, name: 'Annual Plan — 4 Bins',      sku: 'annual-4bin'       },
    // Monthly plans
    { cents: 2999,  name: 'Monthly Plan — 1 Bin',      sku: 'monthly-1bin'      },
    { cents: 3300,  name: 'Monthly Plan — 1 Bin',      sku: 'monthly-1bin'      },
    { cents: 3900,  name: 'Monthly Plan — 2 Bins',     sku: 'monthly-2bin'      },
    { cents: 4500,  name: 'Monthly Plan — 3 Bins',     sku: 'monthly-3bin'      },
    { cents: 5000,  name: 'Monthly Plan — 4 Bins',     sku: 'monthly-4bin'      },
    // Quarterly plans
    { cents: 9400,  name: 'Quarterly Plan — 1 Bin',    sku: 'quarterly-1bin'    },
    { cents: 12400, name: 'Quarterly Plan — 2 Bins',   sku: 'quarterly-2bin'    },
    { cents: 12500, name: 'Quarterly Plan — 2 Bins',   sku: 'quarterly-2bin'    },
    { cents: 14900, name: 'Quarterly Plan — 3 Bins',   sku: 'quarterly-3bin'    },
    { cents: 15500, name: 'Quarterly Plan — 3 Bins',   sku: 'quarterly-3bin'    },
    { cents: 16400, name: 'Quarterly Plan — 4 Bins',   sku: 'quarterly-4bin'    },
    { cents: 18500, name: 'Quarterly Plan — 4 Bins',   sku: 'quarterly-4bin'    },
    // One-time cleans
    { cents: 5499,  name: 'One-Time Clean — 1 Bin',    sku: 'onetime-1bin'      },
    { cents: 6500,  name: 'One-Time Clean — 3 Bins',   sku: 'onetime-3bin'      },
    { cents: 7500,  name: 'One-Time Clean — 4 Bins',   sku: 'onetime-4bin'      },
  ];
  const match = table.find(r => r.cents === amt);
  if (match) return { name: match.name, sku: match.sku };
  // Fallback: generic label with raw amount so Hyros at least gets a real dollar figure
  const dollars = (amt / 100).toFixed(2);
  return { name: 'Jobber Service — $' + dollars, sku: 'jobber-unknown' };
}

/**
 * Look up a GHL contact by email and return { tags, hyrosId, contact } (or defaults).
 * Uses the /contacts/search/duplicate endpoint.
 */
async function ghlFetchContactByEmail(token, locId, email) {
  const em = String(email || '').trim().toLowerCase();
  if (!em || !em.includes('@')) return { tags: [], hyrosId: '' };
  const url = new URL(GHL_API_BASE + '/contacts/search/duplicate');
  url.searchParams.set('locationId', locId);
  url.searchParams.set('email', em);
  let res = await fetch(url.toString(), {
    method: 'GET',
    headers: { ...ghlAuthHeaders(token), Accept: 'application/json' },
  });
  if (!res.ok) {
    res = await fetch(GHL_API_BASE + '/contacts/search/duplicate', {
      method: 'POST',
      headers: ghlAuthHeaders(token),
      body: JSON.stringify({ locationId: locId, email: em }),
    });
  }
  const text = await res.text();
  let j = {};
  try { j = text ? JSON.parse(text) : {}; } catch { j = {}; }
  const contact = ghlExtractContactFromDuplicateJson(j);
  if (!contact) return { tags: [], hyrosId: '' };
  const raw = contact.tags;
  const tags = Array.isArray(raw)
    ? raw.map(t => (typeof t === 'string' ? t.trim() : typeof t === 'object' && t && typeof t.name === 'string' ? t.name.trim() : '')).filter(Boolean)
    : [];
  const hyrosId = ghlExtractHyrosId(contact);
  return { tags, hyrosId, contact };
}

/**
 * Look up a GHL contact by phone and return { tags, hyrosId, contact } (or defaults).
 * Fallback for /jobber-sale when email lookup finds no contact (phone-only opt-in leads).
 * NOTE: GHL /contacts/search/duplicate does NOT support phone param — use /contacts/?query= instead.
 */
async function ghlFetchContactByPhone(token, locId, phone) {
  const ph = String(phone || '').trim();
  if (!ph) return { tags: [], hyrosId: '' };
  // Strip non-digits for the query search, then also try E.164
  const digits = ph.replace(/\D/g, '');
  const searchQuery = digits.length >= 10 ? digits.slice(-10) : digits; // last 10 digits
  const url = new URL(GHL_API_BASE + '/contacts/');
  url.searchParams.set('locationId', locId);
  url.searchParams.set('query', searchQuery);
  url.searchParams.set('limit', '5');
  const res = await fetch(url.toString(), {
    method: 'GET',
    headers: { ...ghlAuthHeaders(token), Accept: 'application/json' },
  });
  const text = await res.text();
  let j = {};
  try { j = text ? JSON.parse(text) : {}; } catch { j = {}; }
  // /contacts/ returns { contacts: [...] }
  const contacts = Array.isArray(j.contacts) ? j.contacts : [];
  // Find the best match — prefer exact phone match
  const e164 = digits.length === 10 ? '+1' + digits
    : digits.length === 11 && digits[0] === '1' ? '+' + digits
    : ph;
  const contact = contacts.find(c => {
    const cp = String(c.phone || '').replace(/\D/g, '');
    return cp.slice(-10) === digits.slice(-10);
  }) || contacts[0] || null;
  if (!contact) return { tags: [], hyrosId: '' };
  const raw = contact.tags;
  const tags = Array.isArray(raw)
    ? raw.map(t => (typeof t === 'string' ? t.trim() : typeof t === 'object' && t && typeof t.name === 'string' ? t.name.trim() : '')).filter(Boolean)
    : [];
  const hyrosId = ghlExtractHyrosId(contact);
  return { tags, hyrosId, contact };
}

/**
 * Extract hyros_id / click_id from a GHL contact object.
 * Checks: (1) customFields array for a field named hyros_id or containing a Hyros-style UUID,
 * (2) the source string for a "hyros:" prefix written by ghlBuildSource.
 */
function ghlExtractHyrosId(contact) {
  if (!contact) return '';
  // Check customFields for a field whose key/name contains hyros or click_id
  const cfs = contact.customFields || contact.customField || [];
  if (Array.isArray(cfs)) {
    for (const cf of cfs) {
      const key = String(cf.id || cf.fieldKey || cf.name || '').toLowerCase();
      const val = String(cf.field_value || cf.fieldValue || cf.value || '').trim();
      if (val && (key.includes('hyros') || key.includes('click_id'))) return val;
    }
  }
  // Fallback: parse "hyros:<value>" from the GHL source string
  const src = String(contact.source || '');
  const m = src.match(/hyros:([^\s\u00b7&·]+)/);
  if (m && m[1]) return m[1];
  return '';
}

/** @deprecated — kept for backward compat; use ghlFetchContactByEmail instead */
async function ghlFetchContactTagsByEmail(token, locId, email) {
  const { tags } = await ghlFetchContactByEmail(token, locId, email);
  return tags;
}

/**
 * POST /jobber-sale
 * Called by the Zapier "HYROS Zapier Jobber" Zap via webhook when a Jobber invoice is paid.
 * Body (from Zapier): {
 *   email: string,          // client email from Jobber invoice
 *   amount: number|string,  // invoice total in dollars (e.g. 29.99)
 *   invoice_number: string, // Jobber invoice number (used as orderId)
 *   first_name?: string,
 *   last_name?: string,
 *   phone?: string,
 *   invoice_date?: string,  // ISO date string
 *   secret?: string,        // optional shared secret for basic auth
 * }
 * Logic:
 *   1. Map amount → product name
 *   2. Look up contact in GHL by email → check for 'purchasefunnellead' tag
 *   3. Fire Hyros Create Lead + Create Order with correct attribution tag
 */
async function handleJobberSale(request, env) {
  try {
    const raw = await request.text();
    if (raw.length > 16384) {
      return json({ ok: false, error: 'Payload too large' }, 413, corsHeaders());
    }
    let body;
    try { body = raw ? JSON.parse(raw) : {}; } catch {
      return json({ ok: false, error: 'Invalid JSON' }, 400, corsHeaders());
    }
    if (!body || typeof body !== 'object') {
      return json({ ok: false, error: 'Invalid body' }, 400, corsHeaders());
    }

    // Optional shared-secret guard (set JOBBER_WEBHOOK_SECRET in Worker env)
    const expectedSecret = env && typeof env.JOBBER_WEBHOOK_SECRET === 'string' ? env.JOBBER_WEBHOOK_SECRET.trim() : '';
    if (expectedSecret) {
      const provided = String(body.secret || request.headers.get('X-Webhook-Secret') || '').trim();
      if (provided !== expectedSecret) {
        return json({ ok: false, error: 'Unauthorized' }, 401, corsHeaders());
      }
    }

    const email = String(body.email || '').trim().toLowerCase();
    const amountRaw = body.amount != null ? body.amount : body.total;
    const amountDollars = parseFloat(String(amountRaw || '0').replace(/[^0-9.]/g, '')) || 0;
    const invoiceNumber = String(body.invoice_number || body.invoice_id || '').trim();
    const firstName = String(body.first_name || '').trim();
    const lastName = String(body.last_name || '').trim();
    const phone = String(body.phone || '').trim();
    const invoiceDate = String(body.invoice_date || body.date || '').trim();

    if (!email) {
      return json({ ok: false, error: 'email is required' }, 400, corsHeaders());
    }
    if (amountDollars <= 0) {
      return json({ ok: false, error: 'amount must be a positive number' }, 400, corsHeaders());
    }

    const product = jobberAmountToProduct(amountDollars);
    console.log('[jobber-sale] product mapped', JSON.stringify({ amountDollars, product }));

    // GHL tag lookup — try email first, then fall back to phone for phone-only opt-in leads
    const ghlToken = env && typeof env.GHL_API_TOKEN === 'string' ? env.GHL_API_TOKEN.trim() : '';
    const ghlLocId = env && typeof env.GHL_LOCATION_ID === 'string' ? env.GHL_LOCATION_ID.trim() : '';
    let isFunnelLead = false;
    let ghlHyrosId = '';  // hyros_click_id stored on the GHL contact at opt-in time
    if (ghlToken && ghlLocId) {
      try {
        // Primary: look up by email
        const emailResult = await ghlFetchContactByEmail(ghlToken, ghlLocId, email);
        let tags = emailResult.tags;
        let hyrosId = emailResult.hyrosId;
        let lookupMethod = 'email';

        // Fallback: if email lookup found no purchasefunnellead tag AND we have a phone, try phone
        // This handles leads who opted in with phone only (no email on GHL contact)
        if (!tags.some(t => t.toLowerCase() === 'purchasefunnellead') && phone) {
          const normalizedPhone = phone.replace(/\D/g, '');
          const e164Phone = normalizedPhone.length === 10 ? '+1' + normalizedPhone
            : normalizedPhone.length === 11 && normalizedPhone[0] === '1' ? '+' + normalizedPhone
            : phone;
          const phoneResult = await ghlFetchContactByPhone(ghlToken, ghlLocId, e164Phone);
          if (phoneResult.tags.some(t => t.toLowerCase() === 'purchasefunnellead')) {
            tags = phoneResult.tags;
            hyrosId = phoneResult.hyrosId;
            lookupMethod = 'phone';
          }
        }

        isFunnelLead = tags.some(t => t.toLowerCase() === 'purchasefunnellead');
        ghlHyrosId = hyrosId;
        console.log('[jobber-sale] ghl tags', JSON.stringify({
          email: email.slice(0, 6) + '***',
          phone: phone ? phone.slice(-4) : null,
          lookupMethod,
          tags,
          isFunnelLead,
          hyrosIdFound: !!ghlHyrosId,
        }));
      } catch (eGhl) {
        console.warn('[jobber-sale] ghl lookup failed', eGhl && eGhl.message ? eGhl.message : eGhl);
      }
    } else {
      console.warn('[jobber-sale] GHL env not configured — skipping tag lookup, defaulting to website-organic');
    }

    const attributionTag = isFunnelLead ? '$phone-close-ad-lead' : '$website-organic';
    console.log('[jobber-sale] attribution', JSON.stringify({ attributionTag, isFunnelLead, hasHyrosId: !!ghlHyrosId }));

    // Fire Hyros
    const apiKey = env && typeof env.HYROS_API_KEY === 'string' ? env.HYROS_API_KEY.trim() : '';
    if (!apiKey) {
      return json({ ok: false, error: 'HYROS_API_KEY not configured' }, 503, corsHeaders());
    }
    const hyrosHeaders = { 'API-key': apiKey, 'Content-Type': 'application/json' };

    // Step 1: Upsert lead in Hyros
    // Include phone so Hyros can match the pre-registered phone lead and merge the email onto it.
    // Include clickId (hyros_id from GHL) if available to lock in ad attribution.
    const leadPayload = {
      email,
      firstName: firstName || undefined,
      lastName: lastName || undefined,
      phoneNumber: phone || undefined,
      tags: [attributionTag, '!highlevel'],
    };
    if (ghlHyrosId) leadPayload.clickId = ghlHyrosId;  // stitch ad click attribution
    let leadResult = {};
    try {
      const leadRes = await fetch('https://api.hyros.com/v1/api/v1.0/leads', {
        method: 'POST',
        headers: hyrosHeaders,
        body: JSON.stringify(leadPayload),
      });
      const leadText = await leadRes.text();
      try { leadResult = JSON.parse(leadText); } catch { leadResult = { raw: leadText.slice(0, 300) }; }
      console.log('[jobber-sale] hyros lead upsert', leadRes.status, JSON.stringify(leadResult).slice(0, 400));
    } catch (eL) {
      console.warn('[jobber-sale] hyros lead upsert error', eL && eL.message ? eL.message : eL);
    }

    // Step 2: Create order in Hyros
    const orderPayload = {
      email,
      orderId: invoiceNumber || ('jobber-' + Date.now()),
      currency: 'USD',
      items: [{ name: product.name, sku: product.sku || undefined, price: amountDollars, quantity: 1 }],
      tags: [attributionTag],
    };
    if (invoiceDate) orderPayload.date = invoiceDate;

    let orderResult = {};
    let orderOk = false;
    try {
      const orderRes = await fetch('https://api.hyros.com/v1/api/v1.0/orders', {
        method: 'POST',
        headers: hyrosHeaders,
        body: JSON.stringify(orderPayload),
      });
      const orderText = await orderRes.text();
      try { orderResult = JSON.parse(orderText); } catch { orderResult = { raw: orderText.slice(0, 300) }; }
      console.log('[jobber-sale] hyros order create', orderRes.status, JSON.stringify(orderResult).slice(0, 400));
      orderOk = orderRes.ok;
    } catch (eO) {
      console.warn('[jobber-sale] hyros order create error', eO && eO.message ? eO.message : eO);
    }

    return json({
      ok: orderOk,
      product: product.name,
      sku: product.sku,
      attribution_tag: attributionTag,
      is_funnel_lead: isFunnelLead,
      invoice_number: invoiceNumber,
      amount_dollars: amountDollars,
      hyros_order: orderResult,
    }, orderOk ? 200 : 502, corsHeaders());

  } catch (err) {
    console.error('[jobber-sale] error', err && err.message ? err.message : err);
    return json({ ok: false, error: 'jobber-sale exception', message: err && err.message ? err.message : String(err) }, 500, corsHeaders());
  }
}

/**
 * POST /stripe-webhook
 * Handles Stripe webhook events. Listens for invoice.payment_succeeded to track
 * subscription renewals in Hyros with recurring: true.
 * Requires STRIPE_WEBHOOK_SECRET in Worker env for signature verification.
 */
async function handleStripeWebhook(request, env) {
  try {
    const rawBody = await request.text();
    const sigHeader = request.headers.get('stripe-signature') || '';
    const webhookSecret = env && typeof env.STRIPE_WEBHOOK_SECRET === 'string' ? env.STRIPE_WEBHOOK_SECRET.trim() : '';

    // Signature verification (HMAC-SHA256 via Stripe's t= ts= v1= scheme)
    if (webhookSecret && sigHeader) {
      const parts = {};
      for (const part of sigHeader.split(',')) {
        const [k, v] = part.split('=');
        if (k && v) parts[k.trim()] = v.trim();
      }
      const ts = parts['t'];
      const v1 = parts['v1'];
      if (ts && v1) {
        const signedPayload = ts + '.' + rawBody;
        const enc = new TextEncoder();
        const key = await crypto.subtle.importKey(
          'raw', enc.encode(webhookSecret), { name: 'HMAC', hash: 'SHA-256' }, false, ['sign']
        );
        const sig = await crypto.subtle.sign('HMAC', key, enc.encode(signedPayload));
        const computed = [...new Uint8Array(sig)].map(b => b.toString(16).padStart(2, '0')).join('');
        if (computed !== v1) {
          console.warn('[stripe-webhook] signature mismatch');
          return json({ ok: false, error: 'Invalid signature' }, 400, corsHeaders());
        }
      }
    }

    let event;
    try { event = rawBody ? JSON.parse(rawBody) : {}; } catch {
      return json({ ok: false, error: 'Invalid JSON' }, 400, corsHeaders());
    }

    const eventType = String(event && event.type || '');
    console.log('[stripe-webhook] event', eventType);

    // Only handle subscription renewal invoices
    if (eventType !== 'invoice.payment_succeeded') {
      return json({ ok: true, ignored: true, event_type: eventType }, 200, corsHeaders());
    }

    const invoice = event && event.data && event.data.object;
    if (!invoice) {
      return json({ ok: false, error: 'No invoice object in event' }, 400, corsHeaders());
    }

    // Only process renewals (billing_reason = subscription_cycle), not the initial charge
    const billingReason = String(invoice.billing_reason || '');
    if (billingReason !== 'subscription_cycle') {
      console.log('[stripe-webhook] skipping non-renewal', billingReason);
      return json({ ok: true, ignored: true, billing_reason: billingReason }, 200, corsHeaders());
    }

    const email = String((invoice.customer_email) || '').trim().toLowerCase();
    const amountDollars = (invoice.amount_paid || 0) / 100;
    const invoiceId = String(invoice.id || '').trim();
    const subscriptionId = String(invoice.subscription || '').trim();
    const customerName = String(invoice.customer_name || '').trim();
    const nameParts = customerName.split(/\s+/).filter(Boolean);
    const firstName = nameParts[0] || '';
    const lastName = nameParts.slice(1).join(' ');

    if (!email || amountDollars <= 0) {
      return json({ ok: false, error: 'Missing email or amount in invoice' }, 400, corsHeaders());
    }

    const product = jobberAmountToProduct(amountDollars);
    console.log('[stripe-webhook] renewal', JSON.stringify({
      email: email.slice(0, 6) + '***', amountDollars, product: product.name, invoiceId, subscriptionId
    }));

    const apiKey = env && typeof env.HYROS_API_KEY === 'string' ? env.HYROS_API_KEY.trim() : '';
    if (!apiKey) {
      return json({ ok: false, error: 'HYROS_API_KEY not configured' }, 503, corsHeaders());
    }
    const hyrosHeaders = { 'API-key': apiKey, 'Content-Type': 'application/json' };

    // Upsert lead
    const leadPayload = { email, firstName: firstName || undefined, lastName: lastName || undefined };
    try {
      const lr = await fetch('https://api.hyros.com/v1/api/v1.0/leads', {
        method: 'POST', headers: hyrosHeaders, body: JSON.stringify(leadPayload),
      });
      const lt = await lr.text();
      console.log('[stripe-webhook] hyros lead upsert', lr.status, lt.slice(0, 300));
    } catch (eL) {
      console.warn('[stripe-webhook] hyros lead error', eL && eL.message ? eL.message : eL);
    }

    // Create renewal order with recurring: true
    const orderPayload = {
      email,
      orderId: invoiceId,
      currency: 'USD',
      recurring: true,
      items: [{ name: product.name, sku: product.sku || undefined, price: amountDollars, quantity: 1 }],
      tags: ['$subscription-renewal'],
    };
    if (subscriptionId) orderPayload.subscriptionId = subscriptionId;

    let orderResult = {};
    let orderOk = false;
    try {
      const or = await fetch('https://api.hyros.com/v1/api/v1.0/orders', {
        method: 'POST', headers: hyrosHeaders, body: JSON.stringify(orderPayload),
      });
      const ot = await or.text();
      try { orderResult = JSON.parse(ot); } catch { orderResult = { raw: ot.slice(0, 300) }; }
      console.log('[stripe-webhook] hyros order create', or.status, JSON.stringify(orderResult).slice(0, 400));
      orderOk = or.ok;
    } catch (eO) {
      console.warn('[stripe-webhook] hyros order error', eO && eO.message ? eO.message : eO);
    }

    return json({
      ok: orderOk,
      event_type: eventType,
      billing_reason: billingReason,
      product: product.name,
      amount_dollars: amountDollars,
      invoice_id: invoiceId,
      hyros_order: orderResult,
    }, orderOk ? 200 : 502, corsHeaders());

  } catch (err) {
    console.error('[stripe-webhook] error', err && err.message ? err.message : err);
    return json({ ok: false, error: 'stripe-webhook exception', message: err && err.message ? err.message : String(err) }, 500, corsHeaders());
  }
}

/**
 * Map plan + bins to a human-readable Hyros product name and tag.
 * Must stay in sync with checkout.html TIER object.
 */
function hyrosBuildProduct(plan, bins) {
  const p = String(plan || 'annual').toLowerCase();
  const b = Math.min(4, Math.max(1, parseInt(String(bins), 10) || 1));
  const binLabel = b === 1 ? '1 Bin' : b + ' Bins';
  const prices = {
    annual:    { 1: 199,   2: 250,   3: 299,   4: 349   },
    monthly:   { 1: 33,    2: 39,    3: 45,    4: 50    },
    quarterly: { 1: 125,   2: 125,   3: 155,   4: 185   },
    onetime:   { 1: 54.99, 2: 54.99, 3: 65,    4: 75    },
  };
  const planLabels = {
    annual: 'Annual Plan',
    monthly: 'Monthly Plan',
    quarterly: 'Quarterly Plan',
    onetime: 'One-Time Clean',
  };
  const planLabel = planLabels[p] || 'Annual Plan';
  const price = (prices[p] || prices.annual)[b] || 199;
  const name = planLabel + ' — ' + binLabel;
  // Hyros tag: lowercase, hyphens, no special chars
  const tag = '$online-' + p + '-' + b + 'bin';
  return { name, tag, price };
}

/**
 * POST a new lead + order to Hyros API.
 * Hyros docs: https://api.hyros.com/v1/api/v1.0/
 * - First create/update the lead, then create the order attached to that lead.
 */
async function hyrosRecordPurchase(env, opts) {
  const apiKey = env && typeof env.HYROS_API_KEY === 'string' ? env.HYROS_API_KEY.trim() : '';
  if (!apiKey) {
    console.warn('[hyros] HYROS_API_KEY not configured — skipping');
    return { ok: false, reason: 'no_api_key' };
  }

   const { email, phone, firstName, lastName, plan, bins, amountDollars, paymentIntentId, fbc, hyrosClickId, purchaseSource } = opts;
  const product = hyrosBuildProduct(plan, bins);
  const hyrosHeaders = {
    'API-key': apiKey,
    'Content-Type': 'application/json',
  };
  // Determine source tag: sent-link, funnel-self-purchase, or fallback to $online-purchase
  const sourceTag = purchaseSource === 'sent-link' ? '$sent-link-purchase'
    : purchaseSource === 'funnel' ? '$funnel-self-purchase'
    : '$funnel-self-purchase'; // default for all checkout.html purchases
  // Step 1: Create or update the lead in Hyros
  const leadPayload = {
    email: email || '',
    firstName: firstName || '',
    lastName: lastName || '',
    phoneNumber: phone || '',
    tags: [sourceTag, '!highlevel'],
  };
  if (fbc) leadPayload.fbc = fbc;
  if (hyrosClickId) leadPayload.clickId = hyrosClickId;;

  let leadResult = {};
  try {
    const leadRes = await fetch('https://api.hyros.com/v1/api/v1.0/leads', {
      method: 'POST',
      headers: hyrosHeaders,
      body: JSON.stringify(leadPayload),
    });
    const leadText = await leadRes.text();
    try { leadResult = JSON.parse(leadText); } catch { leadResult = { raw: leadText.slice(0, 300) }; }
    console.log('[hyros] lead upsert', leadRes.status, JSON.stringify(leadResult).slice(0, 400));
  } catch (eL) {
    console.warn('[hyros] lead upsert error', eL && eL.message ? eL.message : eL);
  }

  // Step 2: Create the order in Hyros
  const orderPayload = {
    email: email || '',
    orderId: paymentIntentId || '',
    currency: 'USD',
    items: [{ name: product.name, sku: product.tag || undefined, price: amountDollars, quantity: 1 }],
    tags: [sourceTag],
  };

  let orderResult = {};
  try {
    const orderRes = await fetch('https://api.hyros.com/v1/api/v1.0/orders', {
      method: 'POST',
      headers: hyrosHeaders,
      body: JSON.stringify(orderPayload),
    });
    const orderText = await orderRes.text();
    try { orderResult = JSON.parse(orderText); } catch { orderResult = { raw: orderText.slice(0, 300) }; }
    console.log('[hyros] order create', orderRes.status, JSON.stringify(orderResult).slice(0, 400));
    if (!orderRes.ok) {
      return { ok: false, reason: 'order_api_error', status: orderRes.status, body: orderResult };
    }
  } catch (eO) {
    console.warn('[hyros] order create error', eO && eO.message ? eO.message : eO);
    return { ok: false, reason: 'order_exception', message: eO && eO.message ? eO.message : String(eO) };
  }

  return { ok: true, product: product.name, tag: product.tag, orderId: paymentIntentId };
}

/**
 * POST /confirm-purchase
 * Called by checkout.html after Stripe payment succeeds.
 * Verifies the PaymentIntent with Stripe, then records the order in Hyros.
 * Body: { payment_intent_id, plan, bins, email, phone, first_name, last_name, fbc, hyros_click_id }
 */
async function handleConfirmPurchase(request, env) {
  try {
    const raw = await request.text();
    if (raw.length > 16384) {
      return json({ ok: false, error: 'Payload too large' }, 413, corsHeaders());
    }
    let body;
    try { body = raw ? JSON.parse(raw) : {}; } catch {
      return json({ ok: false, error: 'Invalid JSON' }, 400, corsHeaders());
    }
    if (!body || typeof body !== 'object') {
      return json({ ok: false, error: 'Invalid body' }, 400, corsHeaders());
    }

    const piId = String(body.payment_intent_id || '').trim();
    if (!piId || !/^pi_/.test(piId)) {
      return json({ ok: false, error: 'Invalid payment_intent_id' }, 400, corsHeaders());
    }

    // Verify the PaymentIntent with Stripe to confirm it actually succeeded
    const stripeSecret = env.STRIPE_SECRET_KEY;
    if (!stripeSecret) {
      return json({ ok: false, error: 'STRIPE_SECRET_KEY not configured' }, 503, corsHeaders());
    }
    const stripeRes = await fetch('https://api.stripe.com/v1/payment_intents/' + encodeURIComponent(piId), {
      method: 'GET',
      headers: { Authorization: 'Bearer ' + stripeSecret },
    });
    const stripeText = await stripeRes.text();
    let stripeData;
    try { stripeData = JSON.parse(stripeText); } catch { stripeData = {}; }

    if (!stripeRes.ok || stripeData.status !== 'succeeded') {
      console.warn('[confirm-purchase] stripe verify failed', stripeRes.status, stripeData && stripeData.status);
      return json({ ok: false, error: 'Payment not confirmed by Stripe', stripe_status: stripeData && stripeData.status }, 402, corsHeaders());
    }

    const amountDollars = (stripeData.amount_received || stripeData.amount || 0) / 100;
    const plan = String(body.plan || stripeData.metadata && stripeData.metadata.plan || 'annual').toLowerCase();
    const bins = parseInt(String(body.bins || stripeData.metadata && stripeData.metadata.bins || '1'), 10) || 1;
    const email = String(body.email || '').trim().toLowerCase();
    const phone = String(body.phone || '').trim();
    const firstName = String(body.first_name || '').trim();
    const lastName = String(body.last_name || '').trim();
    const fbc = String(body.fbc || '').trim();
    const hyrosClickId = String(body.hyros_click_id || '').trim();

    console.log('[confirm-purchase] verified', JSON.stringify({
      piId, plan, bins, amountDollars,
      email: email ? email.slice(0, 6) + '***' : 'none',
      fbc_present: !!fbc,
      hyros_click_id_present: !!hyrosClickId,
    }));

    const purchaseSource = String(body.source || '').trim().toLowerCase() || 'funnel';
    const hyrosResult = await hyrosRecordPurchase(env, {
      email, phone, firstName, lastName, plan, bins, amountDollars, paymentIntentId: piId, fbc, hyrosClickId, purchaseSource,
    });

    console.log('[confirm-purchase] hyros result', JSON.stringify(hyrosResult));

    return json({
      ok: true,
      hyros: hyrosResult,
      product: hyrosResult.product || null,
      stripe_amount: amountDollars,
    }, 200, corsHeaders());

  } catch (err) {
    console.error('[confirm-purchase] error', err && err.message ? err.message : err);
    return json({ ok: false, error: 'confirm-purchase exception', message: err && err.message ? err.message : String(err) }, 500, corsHeaders());
  }
}

async function forwardLeadReceiptToGoogleSheet(env, payload) {
  const url = env && typeof env.GOOGLE_SHEET_WEBHOOK_URL === 'string'
    ? String(env.GOOGLE_SHEET_WEBHOOK_URL || '').trim()
    : '';
  if (!url || !/^https:\/\//.test(url)) {
    return { forwarded: false, reason: 'GOOGLE_SHEET_WEBHOOK_URL not configured' };
  }
  const res = await fetch(url, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      Accept: 'application/json'
    },
    body: JSON.stringify(payload)
  });
  const text = await res.text();
  if (!res.ok) {
    throw new Error('Google Sheet webhook HTTP ' + res.status + ' ' + (text || '').slice(0, 300));
  }
  return {
    forwarded: true,
    status: res.status,
    body: (text || '').slice(0, 300)
  };
}

async function handleLeadReceipt(request, env) {
  try {
    const raw = await request.text();
    if (raw.length > 32768) {
      return json({ ok: false, error: 'Payload too large' }, 413, corsHeaders());
    }
    let parsed;
    try {
      parsed = raw ? JSON.parse(raw) : {};
    } catch {
      return json({ ok: false, error: 'Invalid JSON' }, 400, corsHeaders());
    }
    if (!parsed || typeof parsed !== 'object') {
      return json({ ok: false, error: 'Invalid body' }, 400, corsHeaders());
    }
    const receiptId = crypto.randomUUID();
    const payload = {
      receiptId,
      stage: String(parsed.stage || parsed.status || parsed.type || '').slice(0, 120),
      source: String(parsed.source || '').slice(0, 120),
      full_name: String(parsed.full_name || parsed.name || '').slice(0, 160),
      phone: String(parsed.phone_e164 || parsed.phone || '').slice(0, 40),
      email: String(parsed.email || '').slice(0, 250),
      service_zip: String(parsed.service_zip || parsed.zip || '').slice(0, 20),
      event_id: String(parsed.event_id || parsed.jobber_event_id || parsed.journey_event_id || '').slice(0, 160),
      page_url: String(parsed.page_url || '').slice(0, 500),
      ts: parsed.ts || Date.now()
    };
    console.warn('[lead-receipt]', JSON.stringify(payload));

    let sheetForward = { forwarded: false, reason: 'not_attempted' };
    try {
      sheetForward = await forwardLeadReceiptToGoogleSheet(env, payload);
      console.log('[lead-receipt] google-sheet forward', JSON.stringify(sheetForward));
    } catch (sheetErr) {
      console.warn(
        '[lead-receipt] google-sheet forward failed',
        sheetErr && sheetErr.message ? String(sheetErr.message) : sheetErr
      );
      sheetForward = {
        forwarded: false,
        reason: sheetErr && sheetErr.message ? String(sheetErr.message) : 'forward_failed'
      };
    }

    return json({ ok: true, receiptId, sheetForward }, 200, corsHeaders());
  } catch (err) {
    console.warn('[lead-receipt] handler error', err && err.message ? String(err.message) : err);
    return json({ ok: false, error: 'Lead receipt handler failed' }, 500, corsHeaders());
  }
}

async function handleGhlLead(request, env) {
  try {
    const leadReceiptId = crypto.randomUUID();
    console.log('[ghl-lead] start', JSON.stringify({ leadReceiptId }));
    const token = env.GHL_API_TOKEN;
    const locId = env.GHL_LOCATION_ID;
    if (!token || typeof token !== 'string' || !locId || typeof locId !== 'string') {
      return json({ ok: false, error: 'GHL_API_TOKEN or GHL_LOCATION_ID not configured', leadReceiptId }, 503, corsHeaders());
    }
    console.log('[ghl-lead] env ok');
    const raw = await request.text();
    if (raw.length > GHL_FORWARD_MAX_BYTES) {
      return json({ ok: false, error: 'Payload too large', leadReceiptId }, 413, corsHeaders());
    }
    let parsed;
    try {
      parsed = JSON.parse(raw);
    } catch {
      return json({ ok: false, error: 'Invalid JSON', leadReceiptId }, 400, corsHeaders());
    }
    if (!parsed || typeof parsed !== 'object') {
      return json({ ok: false, error: 'Invalid body', leadReceiptId }, 400, corsHeaders());
    }
    console.log('[ghl-lead] parsed ok');
    const phone = ghlNormalizedPhone(parsed);
    if (!phone) {
      console.warn(
        '[ghl-lead] reject missing phone',
        JSON.stringify({
          leadReceiptId,
          source: String(parsed.source || '').slice(0, 120),
          status: String(parsed.status || '').slice(0, 80),
          type: String(parsed.type || '').slice(0, 80),
          eventId: String(ghlPrimaryEventId(parsed) || '').slice(0, 160),
          serviceZip: String(parsed.service_zip || parsed.zip || '').slice(0, 20)
        })
      );
      return json({ ok: false, error: 'Missing or invalid phone', leadReceiptId }, 400, corsHeaders());
    }
    console.log('[ghl-lead] phone ok');
    const { firstName, lastName, name } = ghlBuildNameParts(parsed);
    const source = ghlBuildSource(parsed);
    const em = parsed.email;
    const emailTrim = em && typeof em === 'string' && em.includes('@') ? em.trim().slice(0, 250) : '';

    let existingContact = null;
    try {
      existingContact = await ghlFetchDuplicateContact(token, locId, phone, emailTrim);
    } catch (eDup) {
      console.warn('[ghl-lead] duplicate fetch error', eDup && eDup.message ? String(eDup.message) : eDup);
    }

    const existingTags =
      existingContact && Array.isArray(existingContact.tags) ? existingContact.tags : [];
    const existingCustom = existingContact ? ghlCustomFieldsFromContact(existingContact) : [];

    const incomingEventRaw = ghlPrimaryEventId(parsed);
    const eventTagStr = ghlEventIdTag(incomingEventRaw);
    const incomingTagList = ghlBuildIncomingTagList(parsed);

    const mergedTags = ghlUnionTags(existingTags, incomingTagList);

    console.log(
      '[ghl-lead] tag merge',
      JSON.stringify({
        existingTagsFound: existingTags,
        incomingTags: incomingTagList,
        incomingEventId: incomingEventRaw || null,
        eventTagGenerated: eventTagStr || null,
        mergedTagsSent: mergedTags,
      })
    );

    const upsertBody = {
      locationId: String(locId).trim(),
      phone,
      name: name.slice(0, 500),
      firstName: firstName.slice(0, 100),
      lastName: (lastName || '').slice(0, 100),
      tags: mergedTags,
      source,
      country: 'US',
    };
    if (emailTrim) {
      upsertBody.email = emailTrim;
    }
    const zip = parsed.service_zip || parsed.zip;
    if (zip != null && String(zip).trim()) {
      upsertBody.postalCode = String(zip).trim().slice(0, 20);
    }
    const incomingCfRows = Array.isArray(parsed.ghl_custom_fields)
      ? parsed.ghl_custom_fields.map(ghlNormalizeCustomFieldRow).filter(Boolean)
      : [];
    const funnelFieldId = env.GHL_FUNNEL_EVENT_FIELD_ID;
    const cfMerged = ghlMergeCustomFieldArrays(
      existingCustom,
      incomingCfRows,
      typeof funnelFieldId === 'string' ? funnelFieldId.trim() : '',
      incomingEventRaw
    );
    if (cfMerged.length) {
      upsertBody.customFields = cfMerged;
    }
    const upsertUrl = GHL_API_BASE + '/contacts/upsert';
    console.log('[ghl-lead] sending upsert', upsertUrl);
    const ghlRes = await fetch(upsertUrl, {
      method: 'POST',
      headers: ghlAuthHeaders(token),
      body: JSON.stringify(upsertBody),
    });
    const ghlText = await ghlRes.text();
    console.log('[ghl-lead] ghl status', ghlRes.status, (ghlText || '').slice(0, 8000));
    let ghlJson;
    try {
      ghlJson = ghlText ? JSON.parse(ghlText) : {};
    } catch {
      ghlJson = { raw: (ghlText || '').slice(0, 400) };
    }
    if (!ghlRes.ok) {
      return json(
        {
          ok: false,
          error: 'GHL contact upsert failed',
          leadReceiptId,
          status: ghlRes.status,
          details: typeof ghlJson === 'object' ? ghlJson : {},
        },
        502,
        corsHeaders()
      );
    }
    const contactId =
      (ghlJson && ghlJson.contact && ghlJson.contact.id) ||
      (ghlJson && ghlJson.id) ||
      (ghlJson && ghlJson.contactId);

    const noteFallbackFieldId = typeof env.GHL_FUNNEL_EVENT_FIELD_ID === 'string' ? env.GHL_FUNNEL_EVENT_FIELD_ID.trim() : '';
    if (!noteFallbackFieldId && incomingEventRaw && contactId) {
      try {
        await ghlCreateContactNote(token, contactId, incomingEventRaw);
      } catch (eNote) {
        console.warn('[ghl-lead] note fallback exception', eNote && eNote.message ? String(eNote.message) : eNote);
      }
    }

    // Pre-register lead in Hyros immediately at opt-in with phone + hyros_click_id.
    // This locks in Meta ad attribution BEFORE the sale happens.
    // When the Jobber invoice fires later, Hyros matches by phone and stitches the order to this click.
    const hyrosApiKey = typeof env.HYROS_API_KEY === 'string' ? env.HYROS_API_KEY.trim() : '';
    const hyrosClickId = String(parsed.hyros_id || '').trim();
    if (hyrosApiKey && phone) {
      try {
        const hyrosLeadPayload = {
          phoneNumber: phone,
          firstName: firstName || undefined,
          lastName: lastName || undefined,
          tags: ['$ghl-new-contact', 'purchasefunnellead'],
        };
        if (hyrosClickId) hyrosLeadPayload.clickId = hyrosClickId;
        if (emailTrim) hyrosLeadPayload.email = emailTrim;
        const hyrosRes = await fetch('https://api.hyros.com/v1/api/v1.0/leads', {
          method: 'POST',
          headers: { 'API-key': hyrosApiKey, 'Content-Type': 'application/json' },
          body: JSON.stringify(hyrosLeadPayload),
        });
        const hyrosText = await hyrosRes.text();
        console.log('[ghl-lead] hyros pre-register', hyrosRes.status, hyrosText.slice(0, 300),
          JSON.stringify({ phone: phone.slice(-4), hasClickId: !!hyrosClickId, hasEmail: !!emailTrim }));
      } catch (eHyros) {
        console.warn('[ghl-lead] hyros pre-register error', eHyros && eHyros.message ? eHyros.message : eHyros);
      }
    } else if (!hyrosApiKey) {
      console.warn('[ghl-lead] HYROS_API_KEY not set — skipping Hyros pre-registration');
    }

    console.log(
      '[ghl-lead] success',
      contactId ? { leadReceiptId, contactId: String(contactId).slice(0, 32) } : { leadReceiptId }
    );
    return json({ ok: true, leadReceiptId, contactId: contactId || undefined }, 200, corsHeaders());
  } catch (e) {
    const msg = e && e.message ? String(e.message) : String(e);
    const stack = e && e.stack ? String(e.stack) : '';
    console.error('[ghl-lead] caught error', msg, stack);
    return json(
      {
        ok: false,
        error: 'ghl-lead exception',
        message: msg,
        stack: stack.slice(0, 2000),
      },
      500,
      corsHeaders()
    );
  }
}

/** Authoritative tier totals (cents) — must match checkout.html / index PRICING. */
function expectedTierCents(plan, bins) {
  const p = String(plan || '').toLowerCase();
  const b = Math.min(4, Math.max(1, parseInt(String(bins), 10) || 1));
  const annual = { 1: 19900, 2: 25000, 3: 29900, 4: 34900 };
  const monthly = { 1: 3300, 2: 3900, 3: 4500, 4: 5000 };
  const quarterly = { 1: 9400, 2: 12400, 3: 14900, 4: 16400 };
  if (p === 'annual') return annual[b] ?? null;
  if (p === 'monthly') return monthly[b] ?? null;
  if (p === 'quarterly') return quarterly[b] ?? null;
  return null;
}

function normalizeCouponInput(raw) {
  return String(raw || '')
    .trim()
    .toUpperCase()
    .replace(/\s+/g, '')
    .replace(/-/g, '')
    .replace(/_/g, '');
}

/** Staff / QA test coupon only — 99% off, min Stripe charge 50¢. Not a public promo. */
function applyStaffTestCoupon(baseCents, rawCoupon) {
  const norm = normalizeCouponInput(rawCoupon);
  if (!norm) return { ok: true, cents: baseCents, tag: '' };
  if (norm === 'NSTEST99') {
    return { ok: true, cents: Math.max(50, Math.floor(baseCents * 0.01)), tag: 'NS_TEST_99' };
  }
  return { ok: false, cents: baseCents, tag: '' };
}

async function sha256HexLower(plain) {
  if (!plain) return '';
  const enc = new TextEncoder();
  const buf = await crypto.subtle.digest('SHA-256', enc.encode(plain));
  return [...new Uint8Array(buf)].map((b) => b.toString(16).padStart(2, '0')).join('');
}

function normalizePhoneDigits(phone) {
  const d = String(phone || '').replace(/\D/g, '');
  if (d.length === 10) return '1' + d;
  if (d.length === 11 && d[0] === '1') return d;
  return d;
}

function normalizeZipDigits(z) {
  return String(z || '')
    .replace(/\D/g, '')
    .slice(0, 5);
}

async function hashUserDataPlain(plain) {
  const out = {};
  if (!plain || typeof plain !== 'object') return out;
  if (plain.email) {
    const em = String(plain.email).trim().toLowerCase();
    if (em) out.em = [await sha256HexLower(em)];
  }
  if (plain.phone || plain.phone_e164) {
    const ph = normalizePhoneDigits(plain.phone || plain.phone_e164);
    if (ph.length >= 11) out.ph = [await sha256HexLower(ph)];
  }
  if (plain.fn || plain.first_name) {
    const fn = String(plain.fn || plain.first_name || '')
      .trim()
      .toLowerCase()
      .split(/\s+/)[0];
    if (fn) out.fn = [await sha256HexLower(fn)];
  }
  if (plain.ln || plain.last_name) {
    const ln = String(plain.ln || plain.last_name || '')
      .trim()
      .toLowerCase()
      .split(/\s+/)
      .filter(Boolean)
      .join(' ');
    if (ln) out.ln = [await sha256HexLower(ln)];
  }
  const zp = normalizeZipDigits(plain.zp || plain.zip || plain.postal_code);
  if (zp.length === 5) out.zp = [await sha256HexLower(zp)];
  if (plain.external_id) {
    const externalId = String(plain.external_id).trim().toLowerCase();
    if (externalId) out.external_id = [await sha256HexLower(externalId)];
  }
  return out;
}

function sanitizeMetaTestEventCode(raw) {
  const s = String(raw || '').trim();
  if (!s || s.length > 40) return '';
  if (!/^TEST[A-Z0-9]+$/i.test(s)) return '';
  return s;
}

async function handleMetaCapi(request, env) {
  const token = env.META_CAPI_ACCESS_TOKEN;
  if (!token || typeof token !== 'string') {
    return json({ error: 'META_CAPI_ACCESS_TOKEN not configured' }, 500, corsHeaders());
  }

  let body;
  try {
    body = await request.json();
  } catch {
    return json({ error: 'Invalid JSON' }, 400, corsHeaders());
  }

  const pixelId = String(body.pixel_id || env.META_PIXEL_ID || '499919262310418');
  const eventName = String(body.event_name || '').trim();
  if (!eventName) {
    return json({ error: 'event_name required' }, 400, corsHeaders());
  }

  const eventId = String(body.event_id || '').trim();
  const eventSourceUrl = String(body.event_source_url || request.headers.get('Referer') || '').slice(0, 2048);
  const customData = body.custom_data && typeof body.custom_data === 'object' ? body.custom_data : {};

  const ip =
    request.headers.get('CF-Connecting-IP') ||
    request.headers.get('True-Client-IP') ||
    (request.headers.get('X-Forwarded-For') || '').split(',')[0].trim() ||
    '';
  const ua = request.headers.get('User-Agent') || '';

  const userData = await hashUserDataPlain(body.user_data_plain || {});
  if (ip) userData.client_ip_address = ip;
  if (ua) userData.client_user_agent = ua;
  if (body.fbp) userData.fbp = String(body.fbp);
  if (body.fbc) userData.fbc = String(body.fbc);

  const payload = {
    data: [
      {
        event_name: eventName,
        event_time: Math.floor(Date.now() / 1000),
        event_id: eventId || undefined,
        action_source: 'website',
        event_source_url: eventSourceUrl,
        user_data: userData,
        custom_data: customData,
      },
    ],
  };

  const testEventCode =
    sanitizeMetaTestEventCode(env.META_TEST_EVENT_CODE) ||
    sanitizeMetaTestEventCode(body.test_event_code);
  if (testEventCode) {
    payload.test_event_code = testEventCode;
  }

  const graphUrl =
    'https://graph.facebook.com/v21.0/' +
    encodeURIComponent(pixelId) +
    '/events?access_token=' +
    encodeURIComponent(token);

  console.log(
    '[meta-capi] dispatch',
    JSON.stringify({
      event_name: eventName,
      event_id: eventId || null,
      pixel_id: pixelId,
      journey_event_id: customData && customData.journey_event_id ? customData.journey_event_id : null,
      session_id: customData && customData.session_id ? customData.session_id : null,
      client_ip_address: ip || null,
      client_user_agent: ua ? String(ua).slice(0, 240) : null,
      fbp_present: !!body.fbp,
      fbc_present: !!body.fbc,
      user_data_keys: Object.keys(userData || {}).sort(),
      custom_data_keys: Object.keys(customData || {}).sort(),
      event_source_url: eventSourceUrl || null,
      test_event_code: testEventCode || null,
    })
  );

  const res = await fetch(graphUrl, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(payload),
  });

  const text = await res.text();
  let parsed;
  try {
    parsed = JSON.parse(text);
  } catch {
    parsed = { raw: text.slice(0, 500) };
  }

  if (!res.ok) {
    console.warn(
      '[meta-capi] error',
      JSON.stringify({
        event_name: eventName,
        event_id: eventId || null,
        status: res.status,
        fbtrace_id: parsed && parsed.fbtrace_id ? parsed.fbtrace_id : null,
        meta: parsed,
      })
    );
    return json({ error: 'Meta CAPI error', meta: parsed }, res.status >= 400 && res.status < 600 ? res.status : 502, corsHeaders());
  }

  console.log(
    '[meta-capi] ok',
    JSON.stringify({
      event_name: eventName,
      event_id: eventId || null,
      status: res.status,
      events_received: parsed.events_received,
      fbtrace_id: parsed.fbtrace_id,
    })
  );

  return json({ ok: true, events_received: parsed.events_received, fbtrace_id: parsed.fbtrace_id }, 200, corsHeaders());
}

export default {
  async fetch(request, env) {
    const h = corsHeaders();

    if (request.method === 'OPTIONS') {
      return new Response(null, { status: 204, headers: h });
    }

    const url = new URL(request.url);
    // Dashboard endpoints allow GET
    if (request.method === 'GET') {
      if (url.pathname === '/dashboard-api' || url.pathname.endsWith('/dashboard-api')) {
        return handleDashboardApi(request, env);
      }
      if (url.pathname === '/dashboard' || url.pathname.endsWith('/dashboard')) {
        return handleDashboardPage(request, env);
      }
    }
    if (request.method !== 'POST') {
      return json({ error: 'Method not allowed' }, 405, h);
    };

    if (isMetaCapiPath(url.pathname)) {
      return handleMetaCapi(request, env);
    }

    if (isGhlLeadPath(url.pathname)) {
      return handleGhlLead(request, env);
    }

    if (isClientObservePath(url.pathname)) {
      return handleClientObserve(request, env);
    }

    if (isLeadReceiptPath(url.pathname)) {
      return handleLeadReceipt(request, env);
    }

    if (isConfirmPurchasePath(url.pathname)) {
      return handleConfirmPurchase(request, env);
    }

    if (isJobberSalePath(url.pathname)) {
      return handleJobberSale(request, env);
    }

     if (isStripeWebhookPath(url.pathname)) {
      return handleStripeWebhook(request, env);
    }
    if (url.pathname === '/dashboard-api' || url.pathname.endsWith('/dashboard-api')) {
      return handleDashboardApi(request, env);
    }
    if (url.pathname === '/dashboard' || url.pathname.endsWith('/dashboard')) {
      return handleDashboardPage(request, env);
    }
    if (!isCreatePaymentIntentPath(url.pathname)) {
      return new Response('Not found', { status: 404, headers: h });
    }

    const secret = env.STRIPE_SECRET_KEY;
    if (!secret || typeof secret !== 'string') {
      return json({ error: 'Server misconfiguration' }, 500, h);
    }

    let body;
    try {
      body = await request.json();
    } catch {
      return json({ error: 'Invalid JSON body' }, 400, h);
    }

    const rawAmount = typeof body.amount === 'number' ? body.amount : body.charge_cents;
    const currency = (body.currency || 'usd').toString().toLowerCase();

    if (typeof rawAmount !== 'number' || !Number.isInteger(rawAmount) || rawAmount < 50) {
      return json({ error: 'Invalid or missing amount (cents, min 50)' }, 400, h);
    }

    const expectedBase = expectedTierCents(body.plan, body.bins);
    const base = expectedBase !== null ? expectedBase : rawAmount;

    const applied = applyStaffTestCoupon(base, body.coupon);
    if (String(body.coupon || '').trim() && !applied.ok) {
      return json({ error: 'Invalid coupon' }, 400, h);
    }
    const amount = applied.cents;

    if (Math.abs(rawAmount - amount) > 1) {
      return json({ error: 'Amount does not match server price; refresh checkout.' }, 400, h);
    }

    const params = new URLSearchParams();
    params.set('amount', String(amount));
    params.set('currency', currency);
    params.set('automatic_payment_methods[enabled]', 'true');
    params.set('capture_method', body.capture_method === 'manual' ? 'manual' : 'automatic');

    if (body.metadata && typeof body.metadata === 'object' && body.metadata !== null) {
      for (const [k, v] of Object.entries(body.metadata)) {
        if (typeof v !== 'string') continue;
        const key = String(k).slice(0, 40);
        if (!key) continue;
        params.set(`metadata[${key}]`, v.slice(0, 500));
      }
    }
    if (applied.tag) {
      params.set('metadata[ns_coupon]', applied.tag);
      params.set('metadata[ns_base_charge_cents]', String(base));
    }

    const idem =
      request.headers.get('Idempotency-Key') ||
      (typeof body.idempotencyKey === 'string' ? body.idempotencyKey : '');

    const stripeHeaders = {
      Authorization: 'Bearer ' + secret,
      'Content-Type': 'application/x-www-form-urlencoded',
    };
    if (idem) {
      stripeHeaders['Idempotency-Key'] = idem.slice(0, 255);
    }

    const stripeRes = await fetch('https://api.stripe.com/v1/payment_intents', {
      method: 'POST',
      headers: stripeHeaders,
      body: params.toString(),
    });

    const stripeJson = await stripeRes.json();

    if (!stripeRes.ok) {
      return json(
        {
          error: stripeJson.error?.message || 'Stripe error',
          type: stripeJson.error?.type,
        },
        stripeRes.status >= 400 && stripeRes.status < 600 ? stripeRes.status : 502,
        h
      );
    }

    const out = { clientSecret: stripeJson.client_secret };
    if (idem) {
      out.idempotencyKeyEcho = idem;
    }
    return json(out, 200, h);
  },
};

// ─────────────────────────────────────────────────────────────────────────────
// DASHBOARD PAGE — serves dashboard.html inline
// ─────────────────────────────────────────────────────────────────────────────
async function handleDashboardPage(request, env) {
  // Fetch the dashboard HTML from the same origin (Cloudflare Pages or static)
  // For now, redirect to the GitHub Pages / static host URL
  return Response.redirect('https://naturalsanitationsignup.com/dashboard.html', 302);
}

// ─────────────────────────────────────────────────────────────────────────────
// DASHBOARD API — aggregates Hyros + Meta data for the dashboard
// ─────────────────────────────────────────────────────────────────────────────
async function handleDashboardApi(request, env) {
  const h = corsHeaders();
  // Simple key auth
  const key = request.headers.get('X-Dashboard-Key') || '';
  if (key !== 'ns2026dash') {
    return json({ error: 'Unauthorized' }, 401, h);
  }

  const url = new URL(request.url);
  const period = url.searchParams.get('period') || 'mtd';

  // Calculate date range
  const now = new Date();
  let fromDate, toDate;
  toDate = now.toISOString().split('T')[0];
  if (period === 'today') {
    fromDate = toDate;
  } else if (period === '3d') {
    const d = new Date(now); d.setDate(d.getDate() - 2);
    fromDate = d.toISOString().split('T')[0];
  } else if (period === '7d') {
    const d = new Date(now); d.setDate(d.getDate() - 6);
    fromDate = d.toISOString().split('T')[0];
  } else {
    // MTD
    fromDate = new Date(now.getFullYear(), now.getMonth(), 1).toISOString().split('T')[0];
  }

  const hyrosKey = env.HYROS_API_KEY || '';
  const hyrosHeaders = { 'API-key': hyrosKey, 'Content-Type': 'application/json' };
  const metaToken = env.META_ADS_TOKEN || '';
  const metaAccountId = 'act_2659531691011918';
  const ghlToken = env.GHL_API_TOKEN || '';
  const ghlLocId = env.GHL_LOCATION_ID || '';
  // Build GHL date filter for contacts created in the period
  const ghlFromTs = new Date(fromDate + 'T00:00:00.000Z').getTime();
  const ghlToTs = new Date(toDate + 'T23:59:59.999Z').getTime();
  // Fetch Hyros leads, sales, Meta insights, and GHL initiate_checkout count in parallel
  const [leadsRes, salesRes, metaRes, ghlCheckoutRes] = await Promise.allSettled([
    fetch(`https://api.hyros.com/v1/api/v1.0/leads?fromDate=${fromDate}&toDate=${toDate}&limit=500`, { headers: hyrosHeaders }),
    fetch(`https://api.hyros.com/v1/api/v1.0/sales?fromDate=${fromDate}&toDate=${toDate}&limit=500`, { headers: hyrosHeaders }),
    fetch(`https://graph.facebook.com/v19.0/${metaAccountId}/insights?fields=spend,impressions,clicks,reach&date_preset=${period === 'mtd' ? 'this_month' : period === 'today' ? 'today' : period === '7d' ? 'last_7d' : 'last_3_days'}&access_token=${metaToken}`),
    ghlToken && ghlLocId ? fetch('https://services.leadconnectorhq.com/contacts/search', {
      method: 'POST',
      headers: { Authorization: 'Bearer ' + ghlToken, Version: '2021-07-28', 'Content-Type': 'application/json', Accept: 'application/json' },
      body: JSON.stringify({
        locationId: ghlLocId,
        page: 1,
        pageLimit: 100,
        filters: [
          { field: 'tags', operator: 'contains', value: 'initiate_checkout' },
        ],
      })
    }) : Promise.resolve(null),
  ]);
  let leads = [], sales = [], metaData = {}, initiateCheckoutCount = 0;
  try { const d = await leadsRes.value.json(); leads = d.result || []; } catch {}
  try { const d = await salesRes.value.json(); sales = d.result || []; } catch {}
  try {
    const d = await metaRes.value.json();
    metaData = (d.data && d.data[0]) || {};
  } catch {}
  // Count GHL initiate_checkout contacts in the period (client-side date filter)
  try {
    if (ghlCheckoutRes.value) {
      const d = await ghlCheckoutRes.value.json();
      const contacts = d.contacts || [];
      // Filter client-side by dateAdded within the period
      initiateCheckoutCount = contacts.filter(c => {
        const ts = c.dateAdded ? new Date(c.dateAdded).getTime() : 0;
        return ts >= ghlFromTs && ts <= ghlToTs;
      }).length;
    }
  } catch {}
  // Count leads (opt-ins) — all Hyros leads in the date range (every opt-in creates a Hyros lead)
  // Hyros does not preserve custom tags like $ghl-new-contact, so we count all leads directly
  const totalLeads = leads.length;

  // Aggregate sales by source tag
  const purchases = { funnel: 0, sent_link: 0, phone_close: 0, organic: 0, renewal: 0, total: 0 };
  const revenue = { funnel: 0, sent_link: 0, phone_close: 0, organic: 0, renewal: 0, total: 0, ad_attributed: 0, recurring: 0 };
  const phonePlanMix = {};
  let totalOrders = 0;
  let totalRevForAov = 0;

  const TEST_EMAILS = new Set(['test@test.com','test-jobber@test.com','test-renewal@test.com',
    'testphoneclose001@example.com','test-attribution@test.com','test-recurring@test.com',
    'test-date@test.com','test-007@test.com']);
  const seenOrderIds = new Set();
  for (const sale of sales) {
    const saleEmail = (sale.lead && sale.lead.email) || '';
    if (TEST_EMAILS.has(saleEmail) || saleEmail.startsWith('test') || saleEmail.includes('@example.com')) continue;
    // Deduplicate by orderId — Hyros logs both $0 and real price for same order
    const orderId = sale.orderId || '';
    if (orderId && seenOrderIds.has(orderId)) continue;
    if (orderId) seenOrderIds.add(orderId);
    const tags = (sale.lead && sale.lead.tags) || [];
    const price = (sale.usdPrice && sale.usdPrice.price) || 0;
    const productName = (sale.product && sale.product.name) || '';
    const recurring = sale.recurring || false;

    // Skip $0 for AOV
    if (price > 0) { totalRevForAov += price; totalOrders++; }

    if (recurring) {
      purchases.renewal++;
      revenue.renewal += price;
      revenue.recurring += price;
    } else if (tags.includes('$funnel-self-purchase')) {
      purchases.funnel++;
      revenue.funnel += price;
      revenue.ad_attributed += price;
    } else if (tags.includes('$sent-link-purchase')) {
      purchases.sent_link++;
      revenue.sent_link += price;
      revenue.ad_attributed += price;
    } else if (tags.includes('$phone-close-ad-lead')) {
      purchases.phone_close++;
      revenue.phone_close += price;
      revenue.ad_attributed += price;
      // Plan mix for phone closes
      const planKey = productName.includes('Annual') ? 'Annual' :
                      productName.includes('Monthly') ? 'Monthly' :
                      productName.includes('Quarterly') ? 'Quarterly' : 'Other';
      phonePlanMix[planKey] = (phonePlanMix[planKey] || 0) + 1;
    } else {
      purchases.organic++;
      revenue.organic += price;
    }
  }

  purchases.total = purchases.funnel + purchases.sent_link + purchases.phone_close + purchases.organic;
  revenue.total = revenue.funnel + revenue.sent_link + revenue.phone_close + revenue.organic + revenue.renewal;
  revenue.aov = totalOrders > 0 ? Math.round(totalRevForAov / totalOrders) : 0;

  // Meta data
  const metaSpend = parseFloat(metaData.spend || 0);
  const metaImpressions = parseInt(metaData.impressions || 0);
  const metaClicks = parseInt(metaData.clicks || 0);

  // Opt-in rate: leads / (meta clicks * 0.6 as proxy for landing page views)
  // We don't have exact visitor count from Meta CAPI, so we use link clicks as proxy
  const estimatedVisitors = metaClicks || 0;
  const optInRate = estimatedVisitors > 0 ? (totalLeads / estimatedVisitors * 100) : 0;

  // Checkout-to-lead rate
  const checkoutRate = totalLeads > 0 ? Math.round((initiateCheckoutCount / totalLeads) * 1000) / 10 : 0;
  return json({
    period, fromDate, toDate,
    leads: totalLeads,
    initiate_checkout: initiateCheckoutCount,
    checkout_rate: checkoutRate,
    visitors: estimatedVisitors || null,
    opt_in_rate: Math.round(optInRate * 10) / 10,
    purchases,
    revenue,
    phone_plan_mix: phonePlanMix,
    phone_close_rate: null,
    phone_contact_rate: null,
    meta: {
      spend: metaSpend,
      impressions: metaImpressions,
      clicks: metaClicks,
    },
  }, 200, h);
}
