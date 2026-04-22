/**
 * Cloudflare Worker
 *
 * Routes:
 *   POST /create-payment-intent — Stripe PaymentIntent (env STRIPE_SECRET_KEY)
 *   POST /meta-capi — Meta Conversions API (env META_CAPI_ACCESS_TOKEN, never expose to browser)
 *
 * Secrets / vars (Cloudflare dashboard → Worker → Settings → Variables):
 *   STRIPE_SECRET_KEY           — required for PaymentIntents
 *   META_CAPI_ACCESS_TOKEN      — required for /meta-capi (never in the browser)
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
    return json({ error: 'Meta CAPI error', meta: parsed }, res.status >= 400 && res.status < 600 ? res.status : 502, corsHeaders());
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
