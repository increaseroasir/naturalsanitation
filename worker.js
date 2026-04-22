/**
 * Cloudflare Worker
 *
 * Routes:
 *   POST /create-payment-intent — Stripe PaymentIntent (env STRIPE_SECRET_KEY)
 *   POST /meta-capi — Meta Conversions API (env META_CAPI_ACCESS_TOKEN, never expose to browser)
 *   POST /ghl-lead — GoHighLevel Contacts API upsert (env GHL_API_TOKEN, GHL_LOCATION_ID; never in browser)
 *   POST /client-observe — lightweight browser/lead observability sink for debugging failed lead delivery
 *   POST /lead-receipt — Worker-side backup logging of submitted lead details for recovery outside GoHighLevel
 *
 * Secrets / vars (Cloudflare dashboard → Worker → Settings → Variables):
 *   STRIPE_SECRET_KEY           — required for PaymentIntents
 *   META_CAPI_ACCESS_TOKEN      — required for /meta-capi (never in the browser)
 *   GHL_API_TOKEN               — required for /ghl-lead (private integration / sub-account token)
 *   GHL_LOCATION_ID             — required for /ghl-lead (sub-account location id)
 *   GHL_FUNNEL_EVENT_FIELD_ID   — optional custom field id (Contacts) to store raw funnel event_id / jobber id
 *   META_TEST_EVENT_CODE        — optional, e.g. TEST28089 → Meta Graph `test_event_code` (Test Events only)
 *
 * Optional: browser may send `test_event_code` in the JSON body; the Worker forwards it only if it
 * matches /^TEST[A-Z0-9]+$/i (same format as Events Manager test codes).
 */

function corsHeaders() {
  return {
    'Access-Control-Allow-Origin': '*',
    'Access-Control-Allow-Methods': 'POST, OPTIONS',
    'Access-Control-Allow-Headers': 'Content-Type, Idempotency-Key',
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
  const out = ['purchasefunnellead'];
  if (Array.isArray(body.tags)) {
    for (const t of body.tags) {
      const s = typeof t === 'string' ? t.trim() : '';
      if (s) out.push(s);
    }
  }
  const st = String(body.status || '').toLowerCase();
  if (st === 'lead_detail') {
    out.push('partial_lead', 'lead_gate', 'lead_detail');
  }
  if (st === 'partial_lead') {
    out.push('partial_lead');
  }
  if (st === 'paid' || body.paid === true) {
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

async function handleClientObserve(request) {
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
    const payload = {
      kind: String(parsed.kind || '').slice(0, 80),
      stage: String(parsed.stage || '').slice(0, 120),
      source: String(parsed.source || '').slice(0, 120),
      status: String(parsed.status || '').slice(0, 80),
      type: String(parsed.type || '').slice(0, 80),
      event_id: String(parsed.event_id || '').slice(0, 160),
      phone_last4: String(parsed.phone_last4 || '').slice(-4),
      service_zip: String(parsed.service_zip || '').slice(0, 20),
      http_status: parsed.http_status || null,
      error_message: String(parsed.error_message || '').slice(0, 500),
      response_body: String(parsed.response_body || '').slice(0, 500),
      page_url: String(parsed.page_url || '').slice(0, 500),
      ts: parsed.ts || Date.now()
    };
    console.warn('[client-observe]', JSON.stringify(payload));
    return json({ ok: true }, 200, corsHeaders());
  } catch (err) {
    console.warn('[client-observe] handler error', err && err.message ? String(err.message) : err);
    return json({ ok: false, error: 'Observability handler failed' }, 500, corsHeaders());
  }
}

async function handleLeadReceipt(request) {
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
    return json({ ok: true, receiptId }, 200, corsHeaders());
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
  const zp = normalizeZipDigits(plain.zp || plain.zip || plain.postal_code);
  if (zp.length === 5) out.zp = [await sha256HexLower(zp)];
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
    if (eventName === 'Purchase') {
      console.warn(
        '[meta-capi] Purchase Meta error',
        JSON.stringify({ event_id: eventId || null, status: res.status, meta: parsed })
      );
    }
    return json({ error: 'Meta CAPI error', meta: parsed }, res.status >= 400 && res.status < 600 ? res.status : 502, corsHeaders());
  }

  if (eventName === 'Purchase') {
    console.log(
      '[meta-capi] Purchase ok',
      JSON.stringify({
        event_id: eventId || null,
        events_received: parsed.events_received,
        fbtrace_id: parsed.fbtrace_id,
      })
    );
  }

  return json({ ok: true, events_received: parsed.events_received, fbtrace_id: parsed.fbtrace_id }, 200, corsHeaders());
}

export default {
  async fetch(request, env) {
    const h = corsHeaders();

    if (request.method === 'OPTIONS') {
      return new Response(null, { status: 204, headers: h });
    }

    if (request.method !== 'POST') {
      return json({ error: 'Method not allowed' }, 405, h);
    }

    const url = new URL(request.url);

    if (isMetaCapiPath(url.pathname)) {
      return handleMetaCapi(request, env);
    }

    if (isGhlLeadPath(url.pathname)) {
      return handleGhlLead(request, env);
    }

    if (isClientObservePath(url.pathname)) {
      return handleClientObserve(request);
    }

    if (isLeadReceiptPath(url.pathname)) {
      return handleLeadReceipt(request);
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
