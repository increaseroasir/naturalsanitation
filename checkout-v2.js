/* Natural Sanitation — funnel step 2 (payment). PRICING mirrors index-v2.js — keep in sync. */
(function () {
  'use strict';

  var GHL_WEBHOOK_URL =
    'https://services.leadconnectorhq.com/hooks/pit-6925afab-67ff-47f1-b732-63b00ff3d3e8/webhook-trigger/';
  var CRM_LEAD_URL = GHL_WEBHOOK_URL;
  var PAYMENT_API = 'https://ns-payment.increase-roas.workers.dev/create-payment-intent';
  var STRIPE_PK_LIVE =
    'pk_live_51IZeLCHhyjMV1WTXFgxViPoMxrhHcpgXY0tgggShjYE0wI2o88gds9xzvnFzjrw0inNNETA2FxtMxVrsCZxaDPY700Y7GyJLQx';
  var STRIPE_PK_TEST = 'YOUR_STRIPE_PUBLISHABLE_KEY_TEST';
  var META_PIXEL_ID = window.__NS_META_PIXEL_ID || '499919262310418';
  var MANNY_SMS_NUMBER = '(586) 500-6794';

  var PRICING = {
    annual: { 1: 199, 2: 249, 3: 299, 4: 349 },
    monthly: { 1: 33, 2: 39, 3: 45, 4: 50 },
    quarterly: { 1: 94, 2: 124, 3: 149, 4: 164 },
  };

  var ZIP_PIPE =
    '48044,A,Macomb Township,115+ Macomb Township homes|48042,A,Macomb Township,75+ Macomb Township homes|48316,A,Shelby Township,48+ Shelby Twp homes|48315,B,Utica / Shelby Twp,39+ homes on this route|48178,B,South Lyon,34+ South Lyon homes|48047,B,New Baltimore / Chesterfield,34+ homes already on it|48306,B,Rochester / Rochester Hills,33+ Rochester area homes|48236,A,Grosse Pointe Woods/Farms/Shores,32+ homes across Grosse Pointe|48094,B,Washington Township,31+ Washington Twp homes|48038,B,Clinton Township,29+ Clinton Twp homes|48307,B,Rochester,29+ Rochester homes|48309,A,Rochester Hills,28+ Rochester Hills homes|48301,A,Bloomfield Hills,24+ Bloomfield homes|48304,A,Bloomfield Hills,24+ Bloomfield homes|48322,B,West Bloomfield,24+ West Bloomfield homes|48302,A,Bloomfield Hills,23+ Bloomfield homes|48009,A,Birmingham,23+ Birmingham homes — Manny personally onboards new Birmingham customers|48317,B,Utica / Shelby Twp,22+ homes on this route|48045,B,Harrison Township,20+ Harrison Twp homes|48323,B,West Bloomfield,20+ West Bloomfield homes|48088,B,Warren (NE),19+ Warren homes|48051,B,New Baltimore / Macomb,18+ homes on this route|48073,B,Royal Oak,18+ Royal Oak homes|48324,B,West Bloomfield,17+ West Bloomfield homes|48348,B,Clarkston,16+ Clarkston homes|48124,B,Dearborn (west),16+ West Dearborn homes|48314,B,Sterling Heights (NE),15+ homes on this route|48390,B,Walled Lake,13+ Walled Lake homes|48382,B,Commerce Twp / Union Lake,13+ Commerce Twp homes|48313,B,Sterling Heights,13+ Sterling Heights homes|48374,B,Novi (north),13+ Novi homes|48371,B,Oxford,13+ Oxford homes|48363,B,Oakland Township,13+ Oakland Twp homes|48359,B,Lake Orion (south),11+ Lake Orion homes|48065,B,Romeo / Bruce Twp,11+ homes on this route|48098,B,Troy (north),11+ Troy homes|48230,A,Grosse Pointe / Park,10+ Grosse Pointe homes|48360,B,Lake Orion / Oakland Twp,10+ homes on this route|48095,B,Washington Township (north),10+ Washington Twp homes';

  var ZIP_MAP = {};
  ZIP_PIPE.split('|').forEach(function (chunk) {
    var p = chunk.split(',');
    if (p.length < 4) return;
    ZIP_MAP[p[0]] = { tier: p[1], city: p[2], neighbor: p.slice(3).join(',') };
  });

  var params = new URLSearchParams(location.search);
  var SESSION_EVENT_ID = params.get('event_id') || '';
  try {
    if (!SESSION_EVENT_ID) SESSION_EVENT_ID = localStorage.getItem('ns_funnel_event_id') || '';
  } catch (e) {}
  if (!SESSION_EVENT_ID && typeof crypto !== 'undefined' && crypto.randomUUID) {
    SESSION_EVENT_ID = crypto.randomUUID();
  }

  function pixelOpts() {
    return SESSION_EVENT_ID && String(SESSION_EVENT_ID).length ? { eventID: SESSION_EVENT_ID } : {};
  }

  var draft = {
    bins: Math.min(4, Math.max(1, parseInt(params.get('bins') || '2', 10) || 2)),
    plan: ['annual', 'monthly', 'quarterly'].indexOf((params.get('plan') || 'annual').toLowerCase()) >= 0
      ? (params.get('plan') || 'annual').toLowerCase()
      : 'annual',
    zip: (params.get('zip') || '').replace(/\D/g, '').slice(0, 5),
    fullName: (params.get('fullName') || '').trim(),
    phone: (params.get('phone') || '').trim(),
    email: (params.get('email') || '').trim(),
  };

  function getChargeDollars() {
    return PRICING[draft.plan][draft.bins] || PRICING.annual[2];
  }

  function planLabel() {
    if (draft.plan === 'monthly') return 'Monthly Plan';
    if (draft.plan === 'quarterly') return 'Quarterly Plan';
    return 'Annual Plan';
  }

  function firstNameFromFull(s) {
    var t = (s || '').trim().split(/\s+/);
    return t[0] || '';
  }

  function getStripePk() {
    var host = (location.hostname || '').toLowerCase();
    var local = host === 'localhost' || host === '127.0.0.1' || host.endsWith('.local');
    var k = local ? STRIPE_PK_TEST : STRIPE_PK_LIVE;
    if (!k || k.indexOf('YOUR_STRIPE') !== -1 || !/^pk_(test|live)_/.test(k)) return null;
    return k;
  }

  function sha256Hex(plain) {
    if (!plain || !window.crypto || !window.crypto.subtle) return Promise.resolve('');
    return crypto.subtle.digest('SHA-256', new TextEncoder().encode(plain)).then(function (buf) {
      return Array.from(new Uint8Array(buf))
        .map(function (b) {
          return b.toString(16).padStart(2, '0');
        })
        .join('');
    });
  }

  function normalizePhoneForMetaHash(phone) {
    var d = String(phone || '').replace(/\D/g, '');
    if (d.length === 10) return '1' + d;
    if (d.length === 11 && d.charAt(0) === '1') return d;
    return d;
  }

  function updateSummaryUi() {
    var d = getChargeDollars();
    var city = (ZIP_MAP[draft.zip] && ZIP_MAP[draft.zip].city) || 'your area';
    document.getElementById('sum-plan-line').textContent =
      draft.bins + ' bin' + (draft.bins > 1 ? 's' : '') + ' · ' + planLabel();
    document.getElementById('sum-price-line').textContent =
      draft.plan === 'annual'
        ? '$' + d + '/year billed today'
        : draft.plan === 'monthly'
          ? '$' + d + '/month'
          : '$' + d + '/quarter';
    document.getElementById('route-window-line').textContent =
      'Your route: ' + city + '. First clean can be as early as tomorrow.';
    document.getElementById('zip-badge-pay').textContent = draft.zip ? 'ZIP ' + draft.zip : 'ZIP —';
    var payBtn = document.getElementById('pop-pay');
    if (payBtn) payBtn.textContent = 'Pay and lock your route — $' + d;
    if (stripeState.pr) {
      try {
        stripeState.pr.update({
          total: {
            label: planLabel() + ' — Natural Sanitation',
            amount: Math.round(d * 100),
          },
        });
      } catch (e) {}
    }
  }

  var stripeState = {
    stripe: null,
    elements: null,
    cardNumber: null,
    cardExpiry: null,
    cardCvc: null,
    pr: null,
    prButton: null,
    mounted: false,
  };

  function mountStripe() {
    var pk = getStripePk();
    if (!pk || typeof Stripe === 'undefined') return false;
    if (!stripeState.stripe) stripeState.stripe = Stripe(pk);
    if (!stripeState.elements) stripeState.elements = stripeState.stripe.elements();
    var style = {
      base: {
        fontFamily: 'Inter,system-ui,sans-serif',
        fontSize: '16px',
        color: '#0F1115',
        '::placeholder': { color: '#9ca3af' },
      },
      invalid: { color: '#dc2626' },
    };
    if (!stripeState.mounted) {
      stripeState.cardNumber = stripeState.elements.create('cardNumber', { style: style, showIcon: true });
      stripeState.cardExpiry = stripeState.elements.create('cardExpiry', { style: style });
      stripeState.cardCvc = stripeState.elements.create('cardCvc', { style: style });
      stripeState.cardNumber.mount('#card-number-element');
      stripeState.cardExpiry.mount('#card-expiry-element');
      stripeState.cardCvc.mount('#card-cvc-element');
      stripeState.cardNumber.on('change', function (ev) {
        document.getElementById('err-cn').textContent = ev.error ? ev.error.message : '';
      });
      stripeState.cardExpiry.on('change', function (ev) {
        document.getElementById('err-ce').textContent = ev.error ? ev.error.message : '';
      });
      stripeState.cardCvc.on('change', function (ev) {
        document.getElementById('err-cv').textContent = ev.error ? ev.error.message : '';
      });
      stripeState.mounted = true;
    }
    var cents = Math.round(getChargeDollars() * 100);
    if (!stripeState.pr) {
      stripeState.pr = stripeState.stripe.paymentRequest({
        country: 'US',
        currency: 'usd',
        total: { label: planLabel() + ' — Natural Sanitation', amount: cents },
        requestPayerName: true,
        requestPayerEmail: true,
        requestPayerPhone: true,
      });
      stripeState.pr.on('paymentmethod', handleWallet);
      stripeState.pr.canMakePayment().then(function (res) {
        var sec = document.getElementById('express-pay-section');
        if (!res) {
          if (sec) sec.classList.add('hidden');
          return;
        }
        if (sec) sec.classList.remove('hidden');
        if (!stripeState.prButton) {
          stripeState.prButton = stripeState.elements.create('paymentRequestButton', {
            paymentRequest: stripeState.pr,
            style: { paymentRequestButton: { type: 'default', theme: 'dark', height: '48px' } },
          });
        }
        try {
          stripeState.prButton.mount('#payment-request-button');
        } catch (e) {}
      });
    } else {
      stripeState.pr.update({
        total: { label: planLabel() + ' — Natural Sanitation', amount: cents },
      });
    }
    return true;
  }

  function buildPiBody(emailOpt) {
    var cents = Math.round(getChargeDollars() * 100);
    return {
      amount: cents,
      charge_cents: cents,
      currency: 'usd',
      capture_method: 'automatic',
      email: emailOpt || undefined,
      metadata: {
        plan: draft.plan,
        bins: String(draft.bins),
        service_zip: draft.zip,
        source: 'checkout-v2',
        event_id: SESSION_EVENT_ID,
      },
    };
  }

  function isStripePi(id) {
    return typeof id === 'string' && id.indexOf('pi_') === 0;
  }

  function firePurchasePixel(amount, paymentIntentId) {
    if (!isStripePi(paymentIntentId)) {
      return;
    }
    var em = draft.email.toLowerCase().trim();
    var ph = normalizePhoneForMetaHash(draft.phone);
    Promise.all([sha256Hex(em), sha256Hex(ph), sha256Hex(firstNameFromFull(draft.fullName).toLowerCase())]).then(function (hashes) {
      if (typeof fbq === 'undefined') return;
      var match = {};
      if (hashes[0]) match.em = hashes[0];
      if (hashes[1]) match.ph = hashes[1];
      if (hashes[2]) match.fn = hashes[2];
      if (Object.keys(match).length) fbq('init', META_PIXEL_ID, match);
      var opts = pixelOpts();
      fbq(
        'track',
        'Purchase',
        {
          value: amount,
          currency: 'USD',
          content_name: planLabel(),
          num_items: draft.bins,
          transaction_id: paymentIntentId,
        },
        opts
      );
    });
    if (typeof gtag !== 'undefined') {
      gtag('event', 'purchase', {
        transaction_id: paymentIntentId,
        value: amount,
        currency: 'USD',
      });
    }
  }

  function onPaid(name, email, phone, paymentIntentId) {
    if (!isStripePi(paymentIntentId)) {
      alert('Payment could not be confirmed. Please try again or contact support.');
      return;
    }
    var amt = getChargeDollars();
    firePurchasePixel(amt, paymentIntentId);
    if (GHL_WEBHOOK_URL && /^https:\/\//.test(GHL_WEBHOOK_URL)) {
      fetch(GHL_WEBHOOK_URL, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          name: name || draft.fullName,
          email: email || draft.email,
          phone: phone || draft.phone,
          plan: draft.plan,
          bins: draft.bins,
          price: amt,
          service_zip: draft.zip,
          source: 'checkout-v2-paid',
          status: 'paid',
          event_id: SESSION_EVENT_ID,
          stripe_transaction_id: paymentIntentId,
        }),
      }).catch(function () {});
    }
    var city = (ZIP_MAP[draft.zip] && ZIP_MAP[draft.zip].city) || 'Metro Detroit';
    var q = new URLSearchParams({
      firstName: firstNameFromFull(draft.fullName),
      plan: draft.plan,
      bins: String(draft.bins),
      amount: String(amt),
      email: email || draft.email,
      phone: phone || draft.phone,
      city: city,
      zip: draft.zip,
      txn: paymentIntentId,
      event_id: SESSION_EVENT_ID,
      from: 'checkout',
    });
    window.location.href = 'thank-you.html?' + q.toString();
  }

  function handleWallet(ev) {
    var processing = document.getElementById('processing');
    var secret = null;
    processing.classList.add('show');
    fetch(PAYMENT_API, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(buildPiBody(ev.payerEmail)),
    })
      .then(function (r) {
        return r.json();
      })
      .then(function (data) {
        if (!data.clientSecret) throw new Error(data.error || 'No client secret');
        secret = data.clientSecret;
        return stripeState.stripe.confirmCardPayment(
          secret,
          { payment_method: ev.paymentMethod.id },
          { handleActions: false }
        );
      })
      .then(function (result) {
        if (result.error) throw result.error;
        if (result.paymentIntent && result.paymentIntent.status === 'requires_action') {
          return stripeState.stripe.confirmCardPayment(secret);
        }
        return result;
      })
      .then(function (result) {
        if (result && result.error) throw result.error;
        var piObj = result && result.paymentIntent;
        if (!piObj || piObj.status !== 'succeeded' || !isStripePi(piObj.id)) {
          throw new Error('Payment not completed');
        }
        ev.complete('success');
        onPaid(ev.payerName, ev.payerEmail, ev.payerPhone, piObj.id);
      })
      .catch(function (err) {
        try {
          ev.complete('fail');
        } catch (e1) {}
        alert(err.message || 'Payment failed');
      })
      .finally(function () {
        processing.classList.remove('show');
      });
  }

  function wirePlanBins() {
    document.querySelectorAll('[data-pay-plan]').forEach(function (btn) {
      btn.addEventListener('click', function () {
        draft.plan = btn.getAttribute('data-pay-plan');
        document.querySelectorAll('[data-pay-plan]').forEach(function (b) {
          b.classList.toggle('is-on', b === btn);
        });
        updateSummaryUi();
        mountStripe();
      });
    });
    document.querySelectorAll('[data-pay-bins]').forEach(function (btn) {
      btn.addEventListener('click', function () {
        draft.bins = parseInt(btn.getAttribute('data-pay-bins'), 10) || 2;
        document.querySelectorAll('[data-pay-bins]').forEach(function (b) {
          b.classList.toggle('is-on', parseInt(b.getAttribute('data-pay-bins'), 10) === draft.bins);
        });
        updateSummaryUi();
        mountStripe();
      });
    });
  }

  function wirePay() {
    document.getElementById('pop-pay').addEventListener('click', function () {
      var fn = (draft.fullName || document.getElementById('pay-name').value || '').trim();
      var em = (draft.email || document.getElementById('pay-email').value || '').trim();
      var ph = (draft.phone || document.getElementById('pay-phone').value || '').trim();
      var bz = (document.getElementById('pop-bzip').value || '').trim();
      if (!fn) {
        document.getElementById('err-card').textContent = 'Full name required — go back to Step 1 if empty.';
        return;
      }
      if (!/^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(em)) {
        document.getElementById('err-card').textContent = 'Valid email required';
        return;
      }
      if (!/^\d{5}$/.test(bz)) {
        document.getElementById('err-card').textContent = 'Billing ZIP required';
        return;
      }
      if (!getStripePk()) {
        alert('Payment unavailable — try again later');
        return;
      }
      mountStripe();
      if (!stripeState.cardNumber) return alert('Card form not ready — try again');
      var processing = document.getElementById('processing');
      var clientSecret = null;
      processing.classList.add('show');
      document.getElementById('err-card').textContent = '';
      fetch(PAYMENT_API, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(buildPiBody(em)),
      })
        .then(function (r) {
          return r.json();
        })
        .then(function (data) {
          if (!data.clientSecret) throw new Error(data.error || 'Payment setup failed');
          clientSecret = data.clientSecret;
          return stripeState.stripe.confirmCardPayment(clientSecret, {
            payment_method: {
              card: stripeState.cardNumber,
              billing_details: {
                name: fn,
                email: em,
                phone: ph,
                address: { postal_code: bz },
              },
            },
          }, { handleActions: false });
        })
        .then(function (result) {
          if (result.error) throw result.error;
          if (result.paymentIntent && result.paymentIntent.status === 'requires_action') {
            return stripeState.stripe.confirmCardPayment(clientSecret);
          }
          return result;
        })
        .then(function (result) {
          if (result && result.error) throw result.error;
          var piObj = result && result.paymentIntent;
          if (!piObj || piObj.status !== 'succeeded' || !isStripePi(piObj.id)) {
            throw new Error('Payment not completed');
          }
          onPaid(fn, em, ph, piObj.id);
        })
        .catch(function (err) {
          document.getElementById('err-card').textContent = err.message || 'Could not charge card';
        })
        .finally(function () {
          processing.classList.remove('show');
        });
    });
  }

  function gateMissingLead() {
    if (!draft.zip || draft.zip.length !== 5 || !ZIP_MAP[draft.zip]) {
      document.getElementById('gate-msg').textContent =
        'Missing or invalid service ZIP. Go back to the landing page and verify your ZIP.';
      document.getElementById('pay-wrap').style.display = 'none';
      return false;
    }
    document.getElementById('pay-name').value = draft.fullName;
    document.getElementById('pay-email').value = draft.email;
    document.getElementById('pay-phone').value = draft.phone;
    return true;
  }

  document.querySelectorAll('[data-pay-plan]').forEach(function (b) {
    b.classList.toggle('is-on', b.getAttribute('data-pay-plan') === draft.plan);
  });
  document.querySelectorAll('[data-pay-bins]').forEach(function (b) {
    b.classList.toggle('is-on', parseInt(b.getAttribute('data-pay-bins'), 10) === draft.bins);
  });

  if (typeof fbq !== 'undefined') {
    fbq('track', 'InitiateCheckout', {
      value: getChargeDollars(),
      currency: 'USD',
      num_items: draft.bins,
      content_name: 'checkout_v2',
    }, pixelOpts());
  }

  if (gateMissingLead()) {
    updateSummaryUi();
    wirePlanBins();
    mountStripe();
    wirePay();
  }
})();
