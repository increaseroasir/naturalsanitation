/**
 * Cloudflare Worker — POST /create-payment-intent
 *
 * Env: STRIPE_SECRET_KEY (Stripe secret key, e.g. sk_test_… or sk_live_…)
 *
 * Bind as Pages Function (e.g. rename to functions/api/create-payment-intent.js)
 * or attach this script to a Worker route that forwards /create-payment-intent.
 * Point index.html / checkout.html fetch at your deployed origin + path.
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

    const amount = typeof body.amount === 'number' ? body.amount : body.charge_cents;
    const currency = (body.currency || 'usd').toString().toLowerCase();

    if (typeof amount !== 'number' || !Number.isInteger(amount) || amount < 50) {
      return json({ error: 'Invalid or missing amount (cents, min 50)' }, 400, h);
    }

    const params = new URLSearchParams();
    params.set('amount', String(amount));
    params.set('currency', currency);
    params.set('automatic_payment_methods[enabled]', 'true');
    params.set(
      'capture_method',
      body.capture_method === 'manual' ? 'manual' : 'automatic'
    );

    if (body.metadata && typeof body.metadata === 'object' && body.metadata !== null) {
      for (const [k, v] of Object.entries(body.metadata)) {
        if (typeof v !== 'string') continue;
        const key = String(k).slice(0, 40);
        if (!key) continue;
        params.set(`metadata[${key}]`, v.slice(0, 500));
      }
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
