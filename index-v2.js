/* Natural Sanitation — funnel step 1 (ZIP + lead). Step 2: checkout-v2.html */
(function () {
  'use strict';

  var NS_PAYMENT_ORIGIN = 'https://ns-payment.increase-roas.workers.dev';
  var GHL_LEAD_PROXY_URL = NS_PAYMENT_ORIGIN + '/ghl-lead';
  var CRM_LEAD_URL = GHL_LEAD_PROXY_URL;
  var META_PIXEL_ID =
    (typeof window !== 'undefined' && window.__NS_META_PIXEL_ID) || '499919262310418';
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

  function reconcileEventId() {
    var q = new URLSearchParams(location.search);
    var fromUrl = q.get('event_id') || q.get('fb_event_id');
    if (fromUrl) {
      try {
        localStorage.setItem('ns_funnel_event_id', fromUrl);
      } catch (e) {}
      return fromUrl;
    }
    try {
      var s = localStorage.getItem('ns_funnel_event_id');
      if (s) return s;
    } catch (e2) {}
    var fresh = genSessionEventId();
    try {
      localStorage.setItem('ns_funnel_event_id', fresh);
    } catch (e3) {}
    return fresh;
  }

  var SESSION_EVENT_ID = reconcileEventId();

  function pixelOpts() {
    return SESSION_EVENT_ID && String(SESSION_EVENT_ID).length ? { eventID: SESSION_EVENT_ID } : {};
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
    if (d.length === 11 && d.charAt(0) === '1') d = d.slice(1);
    if (d.length > 10) d = d.slice(0, 10);
    if (d.length === 10) return '1' + d;
    return d;
  }

  function collectUtmParams() {
    var q = new URLSearchParams(location.search);
    var keys = [
      'utm_source',
      'utm_medium',
      'utm_campaign',
      'utm_content',
      'utm_term',
      'ad_id',
      'adset_id',
      'placement',
    ];
    var o = {};
    keys.forEach(function (k) {
      var v = q.get(k);
      if (v) o[k] = v;
    });
    return o;
  }

  function persistUtmKeys() {
    var u = collectUtmParams();
    try {
      Object.keys(u).forEach(function (k) {
        sessionStorage.setItem('ns_utm_' + k, u[k]);
      });
    } catch (e) {}
  }

  function readStoredUtms() {
    var keys = [
      'utm_source',
      'utm_medium',
      'utm_campaign',
      'utm_content',
      'utm_term',
      'ad_id',
      'adset_id',
      'placement',
    ];
    var o = {};
    try {
      keys.forEach(function (k) {
        var v = sessionStorage.getItem('ns_utm_' + k);
        if (v) o[k] = v;
      });
    } catch (e) {}
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

  /* Annual / quarterly / monthly totals by bin count (2–4 from rate card; 1 inferred) */
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

  function escapeHtml(s) {
    return String(s)
      .replace(/&/g, '&amp;')
      .replace(/</g, '&lt;')
      .replace(/>/g, '&gt;')
      .replace(/"/g, '&quot;');
  }

  function renderZipWaitlist() {
    return (
      '<div class="zip-bad" role="alert">' +
      "<p><strong>We're not in your area yet — but we're expanding fast.</strong></p>" +
      '<p>Drop your email and we\'ll text you the moment your ZIP goes live.</p>' +
      '<div class="ff-field"><label for="wl-email">Email</label>' +
      '<input type="email" id="wl-email" autocomplete="email" placeholder="you@email.com" /></div>' +
      '<button type="button" class="ff-cta" id="wl-btn" style="margin-top:10px">Notify Me</button>' +
      '</div>'
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

  function firstToken(name) {
    var t = (name || '').trim().split(/\s+/);
    return t[0] || '';
  }

  function buildCheckoutQuery(zip, fullName, phone, email) {
    var q = new URLSearchParams();
    q.set('zip', zip);
    q.set('fullName', fullName);
    q.set('phone', phone);
    if (email) q.set('email', email);
    q.set('event_id', SESSION_EVENT_ID);
    q.set('bins', '2');
    q.set('plan', 'annual');
    var utms = Object.assign({}, readStoredUtms(), collectUtmParams());
    Object.keys(utms).forEach(function (k) {
      q.set(k, utms[k]);
    });
    return q.toString();
  }

  function fireLeadPixel(fullName, phone, email, consent) {
    var em = email.toLowerCase().trim();
    var ph = normalizePhoneForMetaHash(phone);
    var fn = firstToken(fullName).toLowerCase();
    return Promise.all([sha256Hex(em), sha256Hex(ph), sha256Hex(fn)]).then(function (hashes) {
      if (typeof fbq === 'undefined') return;
      var match = {};
      if (hashes[0]) match.em = hashes[0];
      if (hashes[1]) match.ph = hashes[1];
      if (hashes[2]) match.fn = hashes[2];
      if (Object.keys(match).length) fbq('init', META_PIXEL_ID, match);
      fbq(
        'track',
        'Lead',
        {
          content_name: 'Verify ZIP — Step 1',
          value: PRICING.annual[2],
          currency: 'USD',
          consent_tcpa: consent && consent.consent_tcpa,
          consent_timestamp: consent && consent.consent_timestamp,
          consent_method: consent && consent.consent_method,
        },
        pixelOpts()
      );
    });
  }

  function updateRouteBadgeFromZip(zip) {
    var el = document.getElementById('route-badge');
    if (!el) return;
    if (zip && zip.length === 5 && ZIP_MAP[zip]) {
      el.textContent = 'Routing this week · ' + ZIP_MAP[zip].city;
    } else {
      el.textContent = 'Serving this week · Macomb & Oakland County';
    }
  }

  function wireFunnelForm() {
    var form = document.getElementById('funnel-form');
    var msg = document.getElementById('funnel-zip-msg');
    var zipInput = document.getElementById('f-zip');
    if (!form || !zipInput) return;

    var baCap = document.getElementById('ba-route-cap');
    function syncBaCaption(z) {
      if (!baCap) return;
      if (z && z.length === 5 && ZIP_MAP[z]) {
        baCap.textContent = "From today's route in " + ZIP_MAP[z].city + '.';
      } else {
        baCap.textContent = "From today's route in Metro Detroit.";
      }
    }

    zipInput.addEventListener('input', function () {
      var z = zipInput.value.replace(/\D/g, '').slice(0, 5);
      zipInput.value = z;
      updateRouteBadgeFromZip(z);
      syncBaCaption(z);
    });

    form.addEventListener('submit', function (e) {
      e.preventDefault();
      msg.innerHTML = '';
      var zip = (document.getElementById('f-zip').value || '').replace(/\D/g, '').slice(0, 5);
      var fullName = (document.getElementById('f-name').value || '').trim();
      var phone = (document.getElementById('f-phone').value || '').trim();
      var email = (document.getElementById('f-email').value || '').trim();
      var digits = phone.replace(/\D/g, '');

      if (zip.length !== 5) {
        msg.innerHTML = '<p class="ff-err">Enter a valid 5-digit ZIP.</p>';
        return;
      }
      if (!fullName) {
        msg.innerHTML = '<p class="ff-err">Full name is required.</p>';
        return;
      }
      if (digits.length < 10) {
        msg.innerHTML = '<p class="ff-err">Mobile phone is required.</p>';
        return;
      }

      var county = countyFromZip(zip);
      var countyLabel = countyLabels[county];
      var city = (ZIP_MAP[zip] && ZIP_MAP[zip].city) || '';
      var consentTs = new Date().toISOString();
      var consent = {
        consent_tcpa: true,
        consent_timestamp: consentTs,
        consent_method: 'verify_zip_submit',
      };
      var utms = Object.assign({}, readStoredUtms(), collectUtmParams());
      var crmPayload = Object.assign(
        {
          type: 'lead_step1_verify_zip',
          first_name: firstToken(fullName),
          full_name: fullName,
          phone: phone,
          phone_e164: (function () {
            var d = phone.replace(/\D/g, '');
            if (d.length === 11 && d.charAt(0) === '1') d = d.slice(1);
            if (d.length > 10) d = d.slice(0, 10);
            if (d.length === 10) return '+1' + d;
            return phone;
          })(),
          email: email || undefined,
          service_zip: zip,
          zip: zip,
          county: county,
          county_label: countyLabel,
          plan_intent: 'annual',
          bins_count: 2,
          tags: ['partial_lead', 'lead_detail'],
          hyros_id: hyrosIdFromCookie(),
          event_id: SESSION_EVENT_ID,
          jobber_event_id: SESSION_EVENT_ID,
          manny_sms_body:
            'ZIP verified: ' +
            fullName +
            ' in ' +
            (city || 'service area') +
            ' — annual 2 bins. Phone: ' +
            phone +
            '. Event: ' +
            SESSION_EVENT_ID,
          sms_notify_to: MANNY_SMS_NUMBER,
        },
        consent,
        utms
      );

      var btn = form.querySelector('button[type="submit"]');
      if (btn) btn.disabled = true;

      postCrmLead(crmPayload)
        .then(function () {
          return fireLeadPixel(fullName, phone, email, consent);
        })
        .then(function () {
          window.dataLayer = window.dataLayer || [];
          window.dataLayer.push({
            event: 'lead_verify_zip',
            event_id: SESSION_EVENT_ID,
            zip: zip,
          });
          window.location.href = 'checkout-v2.html?' + buildCheckoutQuery(zip, fullName, phone, email);
        })
        .catch(function (err) {
          msg.innerHTML =
            '<p class="ff-err">Could not save. Check connection and try again. ' +
            escapeHtml(err.message || '') +
            '</p>';
        })
        .finally(function () {
          if (btn) btn.disabled = false;
        });
    });
  }

  function wireWaitlistDelegation() {
    var msg = document.getElementById('funnel-zip-msg');
    if (!msg) return;
    msg.addEventListener('click', function (e) {
      if (e.target.id === 'wl-btn' || e.target.closest('#wl-btn')) {
        var wl = document.getElementById('wl-email');
        if (!wl) return;
        var em = (wl.value || '').trim();
        if (!/^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(em)) {
          alert('Enter a valid email');
          return;
        }
        if (GHL_LEAD_PROXY_URL && /^https:\/\//.test(GHL_LEAD_PROXY_URL)) {
          fetch(GHL_LEAD_PROXY_URL, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
              email: em,
              zip: (document.getElementById('f-zip').value || '').trim(),
              source: 'index-v2-waitlist',
              status: 'waitlist',
              event_id: SESSION_EVENT_ID,
            }),
          }).catch(function () {});
        }
        msg.innerHTML = '<div class="zip-ok"><p>Thanks — you\'re on the list.</p></div>';
      }
    });
  }

  function scrollToVerify(e) {
    if (e) e.preventDefault();
    var t = document.getElementById('verify');
    if (t) t.scrollIntoView({ behavior: 'smooth', block: 'start' });
    var z = document.getElementById('f-zip');
    if (z) window.setTimeout(function () { z.focus(); }, 400);
  }

  function setupSticky() {
    var bar = document.getElementById('sticky-m');
    if (!bar) return;
    bar.classList.add('show');
    bar.setAttribute('aria-hidden', 'false');
  }

  function setupExitIntent() {
    if (window.matchMedia('(max-width: 900px)').matches) return;
    var exitShown = false;
    document.addEventListener(
      'mouseout',
      function (e) {
        if (exitShown) return;
        if (!e.relatedTarget && e.clientY < 24) {
          exitShown = true;
          var ex = document.getElementById('exit-overlay');
          if (ex) ex.classList.add('show');
        }
      },
      true
    );
  }

  function wireExit() {
    var dismiss = document.getElementById('exit-dismiss');
    var overlay = document.getElementById('exit-overlay');
    if (dismiss && overlay) {
      dismiss.addEventListener('click', function () {
        overlay.classList.remove('show');
      });
      overlay.addEventListener('click', function (e) {
        if (e.target === overlay) overlay.classList.remove('show');
      });
    }
    var form = document.getElementById('exit-form');
    if (form) {
      form.addEventListener('submit', function (e) {
        e.preventDefault();
        var em = document.getElementById('exit-email');
        if (!em || !/^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(em.value.trim())) return;
        if (GHL_LEAD_PROXY_URL && /^https:\/\//.test(GHL_LEAD_PROXY_URL)) {
          fetch(GHL_LEAD_PROXY_URL, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
              email: em.value.trim(),
              source: 'index-v2-exit-intent',
              status: 'gallery_lead',
              event_id: SESSION_EVENT_ID,
            }),
          }).catch(function () {});
        }
        if (overlay) overlay.classList.remove('show');
        alert('Sent. Check your inbox.');
      });
    }
    document.addEventListener('keydown', function (e) {
      if (e.key !== 'Escape') return;
      if (overlay && overlay.classList.contains('show')) overlay.classList.remove('show');
    });
  }

  persistUtmKeys();
  document.querySelectorAll('a[href="#verify"]').forEach(function (a) {
    a.addEventListener('click', scrollToVerify);
  });

  wireFunnelForm();
  wireWaitlistDelegation();
  setupSticky();
  setupExitIntent();
  wireExit();

  setTimeout(function () {
    trackStandard('ViewContent', { content_name: 'funnel_step1_hero' });
  }, 600);

  trackCustom('FunnelEventId', { event_id: SESSION_EVENT_ID });
})();
