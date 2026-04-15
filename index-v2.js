/* Natural Sanitation — landing v2 (no localStorage/sessionStorage in interactive paths) */
(function () {
  'use strict';

  var GHL_WEBHOOK_URL =
    'https://services.leadconnectorhq.com/hooks/pit-6925afab-67ff-47f1-b732-63b00ff3d3e8/webhook-trigger/';
  /** Lead + Jobber/CRM sync — replace with dedicated Jobber endpoint when available */
  var CRM_LEAD_URL = GHL_WEBHOOK_URL;
  var PAYMENT_API = 'https://ns-payment.increase-roas.workers.dev/create-payment-intent';
  var STRIPE_PK_LIVE =
    'pk_live_51IZeLCHhyjMV1WTXFgxViPoMxrhHcpgXY0tgggShjYE0wI2o88gds9xzvnFzjrw0inNNETA2FxtMxVrsCZxaDPY700Y7GyJLQx';
  var STRIPE_PK_TEST = 'YOUR_STRIPE_PUBLISHABLE_KEY_TEST';
  var META_PIXEL_ID = '499919262310418';
  var MANNY_SMS_NUMBER = '(586) 500-6794';

  function genSessionEventId() {
    if (typeof crypto !== 'undefined' && crypto.randomUUID) {
      return crypto.randomUUID();
    }
    return 'xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx'.replace(/[xy]/g, function (c) {
      var r = (Math.random() * 16) | 0;
      var v = c === 'x' ? r : (r & 0x3) | 0x8;
      return v.toString(16);
    });
  }

  var SESSION_EVENT_ID = genSessionEventId();

  function pixelOpts() {
    return { eventID: SESSION_EVENT_ID };
  }

  function trackStandard(ev, params) {
    if (typeof fbq === 'undefined') return;
    fbq('track', ev, params || {}, pixelOpts());
  }

  function trackCustom(ev, params) {
    if (typeof fbq === 'undefined') return;
    fbq('trackCustom', ev, params || {}, pixelOpts());
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

  function collectUtmParams() {
    var q = new URLSearchParams(location.search);
    var o = {};
    ['utm_source', 'utm_medium', 'utm_campaign', 'utm_content', 'utm_term'].forEach(function (k) {
      var v = q.get(k);
      if (v) o[k] = v;
    });
    return o;
  }

  function hyrosIdFromCookie() {
    var parts = document.cookie.split(';');
    for (var i = 0; i < parts.length; i++) {
      var p = parts[i].trim();
      if (/hyros|click_id|_ha/i.test(p)) {
        var eq = p.indexOf('=');
        if (eq > 0) return decodeURIComponent(p.slice(eq + 1));
      }
    }
    return '';
  }

  var PRICING = {
    annual: { 1: 250, 2: 299, 3: 349, 4: 399 },
    monthly: { 1: 39, 2: 45, 3: 50, 4: 55 },
    quarterly: { 1: 125, 2: 155, 3: 185, 4: 215 },
  };

  var ZIP_PIPE =
    '48044,A,Macomb Township,115+ Macomb Township homes|48042,A,Macomb Township,75+ Macomb Township homes|48316,A,Shelby Township,48+ Shelby Twp homes|48315,B,Utica / Shelby Twp,39+ homes on this route|48178,B,South Lyon,34+ South Lyon homes|48047,B,New Baltimore / Chesterfield,34+ homes already on it|48306,B,Rochester / Rochester Hills,33+ Rochester area homes|48236,A,Grosse Pointe Woods/Farms/Shores,32+ homes across Grosse Pointe|48094,B,Washington Township,31+ Washington Twp homes|48038,B,Clinton Township,29+ Clinton Twp homes|48307,B,Rochester,29+ Rochester homes|48309,A,Rochester Hills,28+ Rochester Hills homes|48301,A,Bloomfield Hills,24+ Bloomfield homes|48304,A,Bloomfield Hills,24+ Bloomfield homes|48322,B,West Bloomfield,24+ West Bloomfield homes|48302,A,Bloomfield Hills,23+ Bloomfield homes|48009,A,Birmingham,23+ Birmingham homes — Manny personally onboards new Birmingham customers|48317,B,Utica / Shelby Twp,22+ homes on this route|48045,B,Harrison Township,20+ Harrison Twp homes|48323,B,West Bloomfield,20+ West Bloomfield homes|48088,B,Warren (NE),19+ Warren homes|48051,B,New Baltimore / Macomb,18+ homes on this route|48073,B,Royal Oak,18+ Royal Oak homes|48324,B,West Bloomfield,17+ West Bloomfield homes|48348,B,Clarkston,16+ Clarkston homes|48124,B,Dearborn (west),16+ West Dearborn homes|48314,B,Sterling Heights (NE),15+ homes on this route|48390,B,Walled Lake,13+ Walled Lake homes|48382,B,Commerce Twp / Union Lake,13+ Commerce Twp homes|48313,B,Sterling Heights,13+ Sterling Heights homes|48374,B,Novi (north),13+ Novi homes|48371,B,Oxford,13+ Oxford homes|48363,B,Oakland Township,13+ Oakland Twp homes|48359,B,Lake Orion (south),11+ Lake Orion homes|48065,B,Romeo / Bruce Twp,11+ homes on this route|48098,B,Troy (north),11+ Troy homes|48230,A,Grosse Pointe / Park,10+ Grosse Pointe homes|48360,B,Lake Orion / Oakland Twp,10+ homes on this route|48095,B,Washington Township (north),10+ Washington Twp homes';

  var ZIP_MAP = {};
  ZIP_PIPE.split('|').forEach(function (chunk) {
    var p = chunk.split(',');
    if (p.length < 4) return;
    var z = p[0];
    var neighbor = p.slice(3).join(',');
    ZIP_MAP[z] = { tier: p[1], city: p[2], neighbor: neighbor };
  });

  var WAYNE = { '48236': 1, '48124': 1, '48230': 1 };
  var OAKLAND = {
    '48178': 1,
    '48306': 1,
    '48307': 1,
    '48309': 1,
    '48301': 1,
    '48304': 1,
    '48322': 1,
    '48302': 1,
    '48009': 1,
    '48323': 1,
    '48324': 1,
    '48348': 1,
    '48390': 1,
    '48382': 1,
    '48374': 1,
    '48371': 1,
    '48363': 1,
    '48359': 1,
    '48065': 1,
    '48098': 1,
    '48360': 1,
    '48073': 1,
  };

  function countyFromZip(z) {
    if (WAYNE[z]) return 'wayne';
    if (OAKLAND[z]) return 'oakland';
    return 'macomb';
  }

  var countyLabels = {
    oakland: 'OAKLAND COUNTY',
    macomb: 'MACOMB COUNTY',
    wayne: 'WAYNE COUNTY',
  };

  var draft = {
    county: null,
    countyLabel: 'SELECT COUNTY',
    zip: '',
    zipOk: false,
    planChosen: false,
    leadSaved: false,
    bins: 1,
    plan: 'annual',
    fn: '',
    ph: '',
    em: '',
  };

  var exitShown = false;
  var scrollMarks = { 25: false, 50: false, 75: false, 100: false };
  var tickerIdx = 0;
  var addPaymentInfoFired = false;
  var TICKER_LINES = [
    '🟢 Sarah from Macomb Township just signed up · 4 minutes ago',
    '🟢 Kevin from Grosse Pointe Woods upgraded to annual · 11 min ago',
    '🟢 Jennifer from Bloomfield Hills booked her route slot · 14 min ago',
    '🟢 Mike from Rochester Hills signed up · 22 min ago',
    '🟢 Amanda from West Bloomfield joined the route · 28 min ago',
    '🟢 David from Shelby Township signed up · 33 min ago',
    '🟢 Lisa from Washington Township upgraded to annual · 41 min ago',
    '🟢 Brian from Macomb Township signed up · 47 min ago',
    '🟢 Megan from Birmingham joined the waitlist · 54 min ago',
    '🟢 Tom from Royal Oak signed up · 1 hour ago',
  ];

  function getStripePk() {
    var host = (location.hostname || '').toLowerCase();
    var local = host === 'localhost' || host === '127.0.0.1' || host.endsWith('.local');
    var k = local ? STRIPE_PK_TEST : STRIPE_PK_LIVE;
    if (!k || k.indexOf('YOUR_STRIPE') !== -1 || !/^pk_(test|live)_/.test(k)) return null;
    return k;
  }

  function weeklySignupDisplay() {
    var now = new Date();
    var day = (now.getDay() + 6) % 7;
    var h = now.getHours() + now.getMinutes() / 60;
    var start = 12;
    var end = 75;
    if (day >= 0 && day <= 4) {
      var frac = (day + h / 24) / 4.5;
      if (frac > 1) frac = 1;
      return Math.round(start + frac * (end - start));
    }
    if (day === 5) return end;
    return Math.max(start, end - (day - 5) * 8);
  }

  function syncWeeklyNums() {
    var n = String(weeklySignupDisplay());
    var a = document.getElementById('weekly-signup-inline');
    var b = document.getElementById('weekly-signup-big');
    if (a) a.textContent = n;
    if (b) b.textContent = n;
  }

  function escapeHtml(s) {
    return String(s)
      .replace(/&/g, '&amp;')
      .replace(/</g, '&lt;')
      .replace(/>/g, '&gt;')
      .replace(/"/g, '&quot;');
  }

  function neighborBold(s) {
    var m = String(s).match(/^(\d+\+?)\s*(.*)$/);
    if (m) return '<strong>' + m[1] + '</strong> ' + escapeHtml(m[2]);
    return escapeHtml(s);
  }

  function onboardingCity(city) {
    return escapeHtml(String(city).split('/')[0].trim());
  }

  function renderZipSuccess(zip) {
    var r = ZIP_MAP[zip];
    if (!r) return '';
    var nb = neighborBold(r.neighbor);
    if (zip === '48009') {
      return (
        '<div class="zip-ok" role="status">' +
        '<p>✅ Yes — Birmingham is one of our premium routes.</p>' +
        '<p><strong>23+</strong> Birmingham homes — Manny personally onboards new Birmingham customers.</p>' +
        '</div>'
      );
    }
    if (r.tier === 'A') {
      return (
        '<div class="zip-ok" role="status">' +
        '<p>✅ Yes — ' +
        escapeHtml(r.city) +
        ' is one of our premium routes.</p>' +
        '<p>' +
        nb +
        ' are already on it.</p>' +
        '<p>Founder Manny personally onboards new ' +
        onboardingCity(r.city) +
        ' customers.</p></div>'
      );
    }
    return (
      '<div class="zip-ok" role="status">' +
      '<p>✅ Yes — ' +
      escapeHtml(r.city) +
      ' route.</p><p>' +
      nb +
      ' already signed up.</p>' +
      '<p>Welcome to the neighborhood thread.</p></div>'
    );
  }

  function renderZipWaitlist() {
    return (
      '<div class="zip-bad" role="alert">' +
      "<p><strong>We're not in your area yet — but we're expanding fast.</strong></p>" +
      '<p>Drop your email and we\'ll text you the moment your ZIP goes live.</p>' +
      '<div class="field"><label for="wl-email">Email</label>' +
      '<input type="email" id="wl-email" autocomplete="email" placeholder="you@email.com" /></div>' +
      '<button type="button" class="btn-teal" id="wl-btn" style="max-width:none;margin-top:8px">Notify Me</button>' +
      '</div>'
    );
  }

  function getChargeDollars() {
    return PRICING[draft.plan][draft.bins] || PRICING.annual[1];
  }

  function planLabel() {
    if (draft.plan === 'monthly') return 'Monthly Plan';
    if (draft.plan === 'quarterly') return 'Quarterly Plan';
    return 'Annual Plan';
  }

  function planIntentSlug() {
    return draft.plan;
  }

  function updatePlanTiles() {
    var b = draft.bins;
    document.getElementById('pt-q').textContent =
      '$' + PRICING.quarterly[b] + ' / quarter (' + b + ' bin' + (b > 1 ? 's' : '') + ')';
    document.getElementById('pt-a').textContent =
      '$' + PRICING.annual[b] + ' / year (' + b + ' bin' + (b > 1 ? 's' : '') + ')';
    document.getElementById('pt-m').textContent =
      '$' + PRICING.monthly[b] + ' / month (' + b + ' bin' + (b > 1 ? 's' : '') + ')';
    var btn = document.getElementById('pop-pay');
    var d = getChargeDollars();
    if (draft.plan === 'annual') btn.textContent = 'Lock In My Route Slot — $' + d + '/year';
    else if (draft.plan === 'monthly') btn.textContent = 'Try Monthly — $' + d + '/month';
    else btn.textContent = 'Start Quarterly — $' + d;
  }

  function step4Unlocked() {
    var el = document.getElementById('block-step4');
    return el && el.classList.contains('unlocked');
  }

  function maybeTrackAddPaymentInfo(source) {
    if (addPaymentInfoFired || !draft.leadSaved || !step4Unlocked()) return;
    addPaymentInfoFired = true;
    trackStandard('AddPaymentInfo', {
      content_name: planLabel(),
      value: getChargeDollars(),
      currency: 'USD',
      source: source || 'card',
    });
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
        if (!ev.empty) {
          maybeTrackAddPaymentInfo('card_input');
        }
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
    var z = (document.getElementById('popup-zip').value || '').trim();
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
        service_zip: z,
        source: 'index-v2-modal',
        event_id: SESSION_EVENT_ID,
      },
    };
  }

  function buildMannySmsBody(firstName, city, plan, bins, phone) {
    return (
      'New lead: ' +
      firstName +
      ' in ' +
      city +
      ', wants ' +
      plan +
      ' for ' +
      bins +
      ' bin' +
      (bins > 1 ? 's' : '') +
      '. Phone: ' +
      phone +
      '. Call within 15 min if no purchase.'
    );
  }

  function postCrmLead(payload) {
    if (!CRM_LEAD_URL || !/^https:\/\//.test(CRM_LEAD_URL)) {
      return Promise.reject(new Error('CRM not configured'));
    }
    return fetch(CRM_LEAD_URL, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(payload),
    }).then(function (res) {
      if (!res.ok) {
        return res.text().then(function (t) {
          throw new Error(t || 'CRM save failed (' + res.status + ')');
        });
      }
      return res;
    });
  }

  function fireLeadAfterCrm(consent) {
    var fn = (document.getElementById('pop-fn').value || '').trim();
    var phRaw = (document.getElementById('pop-ph').value || '').trim();
    var phNorm = normalizePhoneForMetaHash(phRaw);
    return sha256Hex(phNorm).then(function (phH) {
      return sha256Hex(fn.toLowerCase().trim()).then(function (fnH) {
        if (typeof fbq !== 'undefined') {
          fbq('init', META_PIXEL_ID, { ph: phH, fn: fnH });
          fbq(
            'track',
            'Lead',
            {
              content_name: planLabel(),
              value: getChargeDollars(),
              currency: 'USD',
              consent_tcpa: consent.consent_tcpa,
              consent_timestamp: consent.consent_timestamp,
              consent_method: consent.consent_method,
            },
            pixelOpts()
          );
        }
        window.dataLayer = window.dataLayer || [];
        window.dataLayer.push({
          event: 'lead_crm_saved',
          event_id: SESSION_EVENT_ID,
          consent_tcpa: consent.consent_tcpa,
          consent_timestamp: consent.consent_timestamp,
          consent_method: consent.consent_method,
          plan: draft.plan,
          bins: draft.bins,
        });
      });
    });
  }

  function handleWallet(ev) {
    maybeTrackAddPaymentInfo('wallet');
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
        ev.complete('success');
        onPaid(ev.payerName, ev.payerEmail, ev.payerPhone);
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

  function onPaid(name, email, phone) {
    if (GHL_WEBHOOK_URL && /^https:\/\//.test(GHL_WEBHOOK_URL)) {
      fetch(GHL_WEBHOOK_URL, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          name: name,
          email: email,
          phone: phone,
          plan: draft.plan,
          bins: draft.bins,
          price: getChargeDollars(),
          service_zip: (document.getElementById('popup-zip').value || '').trim(),
          source: 'index-v2-paid',
          status: 'paid',
          event_id: SESSION_EVENT_ID,
        }),
      }).catch(function () {});
    }
    var z = (document.getElementById('popup-zip').value || '').trim();
    var city = (ZIP_MAP[z] && ZIP_MAP[z].city) || 'Metro Detroit';
    var first = (document.getElementById('pop-fn').value || '').trim() || (name || '').split(/\s+/)[0] || '';
    var q = new URLSearchParams({
      firstName: first,
      plan: draft.plan,
      bins: String(draft.bins),
      amount: String(getChargeDollars()),
      email: email || (document.getElementById('pop-em').value || '').trim(),
      phone: phone || (document.getElementById('pop-ph').value || '').trim(),
      city: city,
      zip: z,
      txn: 'v2_' + Date.now(),
      event_id: SESSION_EVENT_ID,
    });
    window.location.href = 'thank-you.html?' + q.toString();
  }

  function openModal() {
    var m = document.getElementById('signup-modal');
    m.classList.add('show');
    m.setAttribute('aria-hidden', 'false');
    document.body.style.overflow = 'hidden';
    document.getElementById('modal-county-lbl').textContent =
      draft.countyLabel || 'SELECT COUNTY';
    restoreDraftToFields();
    if (draft.zipOk && draft.zip && ZIP_MAP[draft.zip]) {
      var res = document.getElementById('zip-result');
      if (res) res.innerHTML = renderZipSuccess(draft.zip);
      document.getElementById('block-step2').classList.add('unlocked');
    }
    if (draft.planChosen) {
      document.getElementById('block-step3').classList.add('unlocked');
    } else {
      document.getElementById('block-step3').classList.remove('unlocked');
    }
    if (draft.leadSaved) {
      document.getElementById('block-step4').classList.add('unlocked');
      mountStripe();
    } else {
      document.getElementById('block-step4').classList.remove('unlocked');
    }
    document.querySelectorAll('.plan-tile').forEach(function (t) {
      t.classList.toggle('selected', t.getAttribute('data-plan') === draft.plan);
    });
    trackStandard('InitiateCheckout', {
      value: getChargeDollars(),
      currency: 'USD',
      num_items: draft.bins,
      content_name: 'signup_popup_open',
    });
  }

  function closeModal() {
    var m = document.getElementById('signup-modal');
    saveFieldsToDraft();
    m.classList.remove('show');
    m.setAttribute('aria-hidden', 'true');
    document.body.style.overflow = '';
  }

  function saveFieldsToDraft() {
    var z = document.getElementById('popup-zip');
    if (z) draft.zip = z.value.trim();
    draft.fn = (document.getElementById('pop-fn') || {}).value || draft.fn;
    draft.ph = (document.getElementById('pop-ph') || {}).value || draft.ph;
    draft.em = (document.getElementById('pop-em') || {}).value || draft.em;
  }

  function restoreDraftToFields() {
    var z = document.getElementById('popup-zip');
    if (z && draft.zip) z.value = draft.zip;
    var fn = document.getElementById('pop-fn');
    if (fn && draft.fn) fn.value = draft.fn;
    var ph = document.getElementById('pop-ph');
    if (ph && draft.ph) ph.value = draft.ph;
    var em = document.getElementById('pop-em');
    if (em && draft.em) em.value = draft.em;
  }

  function scrollToCounty() {
    var el = document.getElementById('county-selector');
    if (el) el.scrollIntoView({ behavior: 'smooth', block: 'start' });
  }

  function setupSticky() {
    var bar = document.getElementById('sticky-m');
    function onScroll() {
      var maxScroll = Math.max(1, document.body.scrollHeight - window.innerHeight);
      var pct = (window.scrollY / maxScroll) * 100;
      var show = window.scrollY > document.body.scrollHeight * 0.2;
      bar.classList.toggle('show', show);
      bar.setAttribute('aria-hidden', show ? 'false' : 'true');

      var depths = [25, 50, 75, 100];
      depths.forEach(function (d) {
        if (scrollMarks[d]) return;
        if (pct >= d) {
          scrollMarks[d] = true;
          if (typeof gtag !== 'undefined') gtag('event', 'scroll_depth', { depth: d });
        }
      });
    }
    window.addEventListener('scroll', onScroll, { passive: true });
    onScroll();
  }

  function setupExitIntent() {
    if (window.matchMedia('(max-width: 900px)').matches) return;
    document.addEventListener(
      'mouseout',
      function (e) {
        if (exitShown) return;
        if (!e.relatedTarget && e.clientY < 24) {
          exitShown = true;
          document.getElementById('exit-overlay').classList.add('show');
        }
      },
      true
    );
  }

  function setupTicker() {
    var el = document.getElementById('activity-ticker');
    if (!el) return;
    setInterval(function () {
      tickerIdx = (tickerIdx + 1) % TICKER_LINES.length;
      el.textContent = TICKER_LINES[tickerIdx];
    }, 6500);
  }

  function wireCountyTiles() {
    document.querySelectorAll('.tile-county').forEach(function (btn) {
      btn.addEventListener('click', function () {
        draft.county = btn.getAttribute('data-county');
        draft.countyLabel = btn.getAttribute('data-label') || countyLabels[draft.county];
        trackCustom('ClickCTA', { county: draft.county, county_label: draft.countyLabel });
        openModal();
      });
    });
  }

  function wireZipCheck() {
    document.getElementById('popup-check-zip').addEventListener('click', function () {
      var zip = (document.getElementById('popup-zip').value || '').replace(/\D/g, '').slice(0, 5);
      document.getElementById('popup-zip').value = zip;
      var res = document.getElementById('zip-result');
      if (zip.length !== 5) {
        draft.zipOk = false;
        res.innerHTML = '<p class="zip-bad">Enter a valid 5-digit ZIP.</p>';
        return;
      }
      if (!draft.county) {
        draft.county = countyFromZip(zip);
        draft.countyLabel = countyLabels[draft.county];
        document.getElementById('modal-county-lbl').textContent = draft.countyLabel;
      }
      if (typeof gtag !== 'undefined') gtag('event', 'zip_entered', { zip: zip });
      if (ZIP_MAP[zip]) {
        draft.zipOk = true;
        draft.zip = zip;
        res.innerHTML = renderZipSuccess(zip);
        document.getElementById('block-step2').classList.add('unlocked');
        document.getElementById('block-step2').scrollIntoView({ behavior: 'smooth', block: 'nearest' });
      } else {
        draft.zipOk = false;
        res.innerHTML = renderZipWaitlist();
      }
    });
  }

  function submitWaitlist() {
    var wl = document.getElementById('wl-email');
    if (!wl) return;
    var em = (wl.value || '').trim();
    if (!/^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(em)) {
      alert('Enter a valid email');
      return;
    }
    if (GHL_WEBHOOK_URL && /^https:\/\//.test(GHL_WEBHOOK_URL)) {
      fetch(GHL_WEBHOOK_URL, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          email: em,
          zip: (document.getElementById('popup-zip').value || '').trim(),
          source: 'index-v2-waitlist',
          status: 'waitlist',
          event_id: SESSION_EVENT_ID,
        }),
      }).catch(function () {});
    }
    document.getElementById('zip-result').innerHTML =
      '<div class="zip-ok"><p>Thanks — you\'re on the list.</p></div>';
  }

  function wireBinsPlans() {
    document.querySelectorAll('.bin-t').forEach(function (b) {
      b.addEventListener('click', function () {
        document.querySelectorAll('.bin-t').forEach(function (x) {
          x.classList.remove('on');
        });
        b.classList.add('on');
        draft.bins = parseInt(b.getAttribute('data-bins'), 10) || 1;
        updatePlanTiles();
        if (stripeState.pr) {
          stripeState.pr.update({
            total: {
              label: planLabel() + ' — Natural Sanitation',
              amount: Math.round(getChargeDollars() * 100),
            },
          });
        }
      });
    });
    document.querySelectorAll('.plan-tile').forEach(function (tile) {
      tile.addEventListener('click', function (e) {
        if (e.target.closest('button')) e.preventDefault();
        var plan = tile.getAttribute('data-plan');
        draft.plan = plan;
        draft.planChosen = true;
        document.querySelectorAll('.plan-tile').forEach(function (t) {
          t.classList.toggle('selected', t === tile);
        });
        trackStandard('AddToCart', {
          content_type: 'product',
          content_ids: [plan],
          contents: [{ id: plan, quantity: draft.bins }],
          value: getChargeDollars(),
          currency: 'USD',
        });
        if (typeof gtag !== 'undefined') gtag('event', 'add_to_cart', { plan: plan, bins: draft.bins });
        updatePlanTiles();
        document.getElementById('block-step3').classList.add('unlocked');
        document.getElementById('block-step3').scrollIntoView({ behavior: 'smooth', block: 'nearest' });
      });
    });
  }

  function wireStepCContinue() {
    document.getElementById('pop-continue-checkout').addEventListener('click', function () {
      var errEl = document.getElementById('step-c-err');
      errEl.textContent = '';
      var fn = (document.getElementById('pop-fn').value || '').trim();
      var ph = (document.getElementById('pop-ph').value || '').trim();
      var digits = ph.replace(/\D/g, '');
      if (!fn) {
        errEl.textContent = 'Please enter your first name.';
        return;
      }
      if (digits.length < 10) {
        errEl.textContent = 'Please enter a valid mobile number.';
        return;
      }
      var zip = (document.getElementById('popup-zip').value || '').trim();
      if (!draft.zipOk || !ZIP_MAP[zip]) {
        errEl.textContent = 'Please verify your ZIP first.';
        return;
      }
      if (!draft.planChosen) {
        errEl.textContent = 'Please select a plan first.';
        return;
      }
      var consentTs = new Date().toISOString();
      var consent = {
        consent_tcpa: true,
        consent_timestamp: consentTs,
        consent_method: 'submit_button_implied',
      };
      var city = (ZIP_MAP[zip] && ZIP_MAP[zip].city) || '';
      var utms = collectUtmParams();
      var crmPayload = Object.assign(
        {
          type: 'lead_pre_checkout',
          first_name: fn,
          phone: ph,
          zip: zip,
          county: draft.county,
          county_label: draft.countyLabel,
          plan_intent: planIntentSlug(),
          bins_count: draft.bins,
          hyros_id: hyrosIdFromCookie(),
          event_id: SESSION_EVENT_ID,
          jobber_event_id: SESSION_EVENT_ID,
          manny_sms_body: buildMannySmsBody(fn, city || 'Metro Detroit', planLabel(), draft.bins, ph),
          sms_notify_to: MANNY_SMS_NUMBER,
        },
        consent,
        utms
      );
      var btn = document.getElementById('pop-continue-checkout');
      btn.disabled = true;
      postCrmLead(crmPayload)
        .then(function () {
          draft.leadSaved = true;
          saveFieldsToDraft();
          return fireLeadAfterCrm(consent).catch(function () {});
        })
        .then(function () {
          document.getElementById('block-step4').classList.add('unlocked');
          document.getElementById('block-step4').scrollIntoView({ behavior: 'smooth', block: 'nearest' });
          addPaymentInfoFired = false;
          mountStripe();
        })
        .catch(function (e) {
          errEl.textContent =
            'We could not save your info. Check your connection and try again. ' + (e.message || '');
        })
        .finally(function () {
          btn.disabled = false;
        });
    });
  }

  function wirePay() {
    document.getElementById('pop-pay').addEventListener('click', function () {
      saveFieldsToDraft();
      if (!draft.leadSaved) {
        alert('Complete the previous step first.');
        return;
      }
      var fn = (document.getElementById('pop-fn').value || '').trim();
      var ph = (document.getElementById('pop-ph').value || '').trim();
      var em = (document.getElementById('pop-em').value || '').trim();
      var bz = (document.getElementById('pop-bzip').value || '').trim();
      if (!fn) return alert('First name required');
      if (!ph || ph.replace(/\D/g, '').length < 10) return alert('Valid mobile required');
      if (!/^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(em)) return alert('Valid email required');
      if (!/^\d{5}$/.test(bz)) return alert('Billing ZIP required');
      if (!getStripePk()) return alert('Payment unavailable — try again later');
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
          onPaid(fn, em, ph);
        })
        .catch(function (err) {
          document.getElementById('err-card').textContent = err.message || 'Could not charge card';
        })
        .finally(function () {
          processing.classList.remove('show');
        });
    });
  }

  function wireValidationHints() {
    function ok(el, cond, msgEl) {
      var m = document.getElementById(msgEl);
      if (!m) return;
      m.textContent = cond ? '✓' : '';
    }
    document.getElementById('pop-fn').addEventListener('input', function () {
      ok(this, this.value.trim().length > 0, 'ok-fn');
    });
    document.getElementById('pop-ph').addEventListener('input', function () {
      ok(this, this.value.replace(/\D/g, '').length >= 10, 'ok-ph');
    });
    document.getElementById('pop-em').addEventListener('input', function () {
      ok(this, /^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(this.value.trim()), 'ok-em');
    });
    document.getElementById('popup-zip').addEventListener('input', function () {
      ok(this, /^\d{5}$/.test(this.value.trim()), 'popup-zip-ok');
    });
  }

  function wireModalChrome() {
    document.getElementById('modal-close').addEventListener('click', closeModal);
    document.getElementById('signup-modal').addEventListener('click', function (e) {
      if (e.target === this) closeModal();
    });
    document.addEventListener('keydown', function (e) {
      if (e.key !== 'Escape') return;
      var ex = document.getElementById('exit-overlay');
      if (ex && ex.classList.contains('show')) {
        ex.classList.remove('show');
        return;
      }
      if (document.getElementById('signup-modal').classList.contains('show')) {
        closeModal();
      }
    });
  }

  function wireZipResultClicks() {
    document.getElementById('zip-result').addEventListener('click', function (e) {
      if (e.target.id === 'wl-btn' || e.target.closest('#wl-btn')) submitWaitlist();
    });
  }

  function wireExit() {
    document.getElementById('exit-dismiss').addEventListener('click', function () {
      document.getElementById('exit-overlay').classList.remove('show');
    });
    document.getElementById('exit-overlay').addEventListener('click', function (e) {
      if (e.target === this) this.classList.remove('show');
    });
    document.getElementById('exit-form').addEventListener('submit', function (e) {
      e.preventDefault();
      var em = document.getElementById('exit-email').value.trim();
      if (!/^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(em)) return;
      if (GHL_WEBHOOK_URL && /^https:\/\//.test(GHL_WEBHOOK_URL)) {
        fetch(GHL_WEBHOOK_URL, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            email: em,
            source: 'index-v2-exit-intent',
            status: 'gallery_lead',
            event_id: SESSION_EVENT_ID,
          }),
        }).catch(function () {});
      }
      document.getElementById('exit-overlay').classList.remove('show');
      alert('Sent. Check your inbox.');
    });
  }

  document.querySelectorAll('a[href="#county-selector"]').forEach(function (a) {
    a.addEventListener('click', function (e) {
      e.preventDefault();
      scrollToCounty();
    });
  });

  setTimeout(function () {
    trackStandard('ViewContent', { content_name: 'hero' });
  }, 1000);

  syncWeeklyNums();
  setInterval(syncWeeklyNums, 60000);
  setupTicker();
  setupSticky();
  setupExitIntent();
  wireCountyTiles();
  wireModalChrome();
  wireZipCheck();
  wireZipResultClicks();
  wireBinsPlans();
  wireStepCContinue();
  wirePay();
  wireValidationHints();
  wireExit();
  updatePlanTiles();
})();
