/**
 * Natural Sanitation — shared browser attribution + Meta Pixel/CAPI bridge.
 * - One persistent journey id (localStorage ns_funnel_event_id) for CRM / Hyros stitching.
 * - Meta browser + server dedupe: standard events use eventID = journeyId + ':' + slug. Purchase uses the Stripe
 *   PaymentIntent id (pi_*) as both Pixel eventID and CAPI event_id so Test Events dedupes browser + server.
 * - CAPI is sent only to your Worker (META_CAPI_ACCESS_TOKEN stays on the server).
 *
 * Config (optional on window before this script):
 *   __NS_META_PIXEL_ID, __NS_CAPI_URL
 *   __NS_META_TEST_EVENT_CODE — e.g. TEST28089; forwarded to the Worker as test_event_code (Meta Test Events stream).
 *     Remove or empty for production once CAPI is verified.
 *   __NS_DEBUG_CAPI — set true to console.log successful CAPI responses (event name + body snippet).
 */
(function (global) {
  'use strict';

  var META_PIXEL_ID = global.__NS_META_PIXEL_ID || '499919262310418';
  var CAPI_URL =
    global.__NS_CAPI_URL ||
    'https://ns-payment.increase-roas.workers.dev/meta-capi';
  var OBSERVE_URL =
    global.__NS_OBSERVE_URL ||
    'https://ns-payment.increase-roas.workers.dev/client-observe';
  var LEAD_RECEIPT_URL =
    global.__NS_LEAD_RECEIPT_URL ||
    'https://ns-payment.increase-roas.workers.dev/lead-receipt';

  var LS_ATTR = 'ns_attribution_bundle';
  var LS_JOURNEY = 'ns_funnel_event_id';
  var SS_SESSION = 'ns_session_id';
  var SS_LEAD_FIRED = 'ns_meta_standard_lead_fired';

  var ATTR_KEYS = [
    'utm_source',
    'utm_medium',
    'utm_campaign',
    'utm_content',
    'utm_term',
    'fbclid',
    'gclid',
    'msclkid'
  ];

  var state = { bundle: {}, bootstrapped: false };
  var BOOT = global.__NS_TRACKING_BOOTSTRAP_MODE || 'full';

  function getCookie(name) {
    var parts = ('; ' + document.cookie).split('; ' + name + '=');
    if (parts.length === 2) {
      return decodeURIComponent(parts.pop().split(';').shift() || '');
    }
    return '';
  }

  function genId() {
    if (global.crypto && crypto.randomUUID) {
      return crypto.randomUUID();
    }
    return 'xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx'.replace(/[xy]/g, function (c) {
      var r = (Math.random() * 16) | 0;
      var v = c === 'x' ? r : (r & 0x3) | 0x8;
      return v.toString(16);
    });
  }

  function readJsonLS(key) {
    try {
      var r = localStorage.getItem(key);
      return r ? JSON.parse(r) : null;
    } catch (e) {
      return null;
    }
  }

  function writeJsonLS(key, o) {
    try {
      localStorage.setItem(key, JSON.stringify(o));
    } catch (e1) {}
  }

  function captureFromUrl() {
    var out = {};
    try {
      var q = new URLSearchParams(location.search);
      ATTR_KEYS.forEach(function (k) {
        var v = q.get(k);
        if (v) {
          out[k] = v;
        }
      });
      var eid = q.get('event_id') || q.get('fb_event_id');
      if (eid) {
        out.imported_event_id = String(eid);
      }
    } catch (e) {}
    return out;
  }

  function mergeBundles(prev, incoming) {
    var o = Object.assign({}, prev || {}, incoming || {});
    o.t = Date.now();
    return o;
  }

  function ensureJourneyId(bundle) {
    var id =
      bundle.journey_event_id ||
      bundle.imported_event_id ||
      (function () {
        try {
          return localStorage.getItem(LS_JOURNEY);
        } catch (e) {
          return null;
        }
      })();
    if (!id) {
      id = genId();
    }
    try {
      localStorage.setItem(LS_JOURNEY, id);
    } catch (e2) {}
    bundle.journey_event_id = id;
    delete bundle.imported_event_id;
    return id;
  }

  function ensureSessionId(bundle) {
    try {
      var s = sessionStorage.getItem(SS_SESSION);
      if (!s) {
        s = genId();
        sessionStorage.setItem(SS_SESSION, s);
      }
      bundle.session_id = s;
    } catch (e) {
      bundle.session_id = bundle.session_id || genId();
    }
  }

  function persistBundle() {
    writeJsonLS(LS_ATTR, state.bundle);
  }

  function syncUrlParams() {
    try {
      var u = new URL(location.href);
      var q = u.searchParams;
      if (state.bundle.journey_event_id && !q.get('event_id')) {
        q.set('event_id', state.bundle.journey_event_id);
      }
      ATTR_KEYS.forEach(function (k) {
        if (state.bundle[k] && !q.get(k)) {
          q.set(k, state.bundle[k]);
        }
      });
      var qs = q.toString();
      history.replaceState({}, '', u.pathname + (qs ? '?' + qs : '') + u.hash);
    } catch (e) {}
  }

  function journeyId() {
    return (state.bundle && state.bundle.journey_event_id) || '';
  }

  function dedupeId(slug) {
    var j = journeyId() || 'nj';
    return j + ':' + String(slug || 'evt');
  }

  function getAttributionPayload() {
    var b = state.bundle || {};
    var utm = {};
    ATTR_KEYS.forEach(function (k) {
      if (b[k]) {
        utm[k] = b[k];
      }
    });
    return {
      journey_event_id: b.journey_event_id,
      session_id: b.session_id,
      service_zip: b.service_zip || '',
      bins: b.bins != null ? b.bins : '',
      plan_intent: b.plan_intent || '',
      county: b.county || '',
      page_url: String(location.href || ''),
      referrer: String(document.referrer || '')
    };
  }

  function updateContext(partial) {
    state.bundle = mergeBundles(state.bundle, partial || {});
    persistBundle();
  }

  function shortText(v, maxLen) {
    var s = String(v == null ? '' : v);
    var m = typeof maxLen === 'number' && maxLen > 0 ? maxLen : 400;
    return s.length > m ? s.slice(0, m) : s;
  }

  function postObserve(kind, payload) {
    if (!OBSERVE_URL || !/^https:\/\//.test(OBSERVE_URL)) {
      return;
    }
    var body = Object.assign(
      {
        kind: String(kind || 'client_event'),
        journey_event_id: journeyId() || '',
        page_url: String(location.href || ''),
        referrer: String(document.referrer || ''),
        user_agent: typeof navigator !== 'undefined' ? String(navigator.userAgent || '') : '',
        ts: Date.now()
      },
      payload || {}
    );
    fetch(OBSERVE_URL, {
      method: 'POST',
      mode: 'cors',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(body),
      keepalive: true
    }).catch(function () {});
  }

  function observeLeadRequest(kind, leadBody, extra) {
    var b = leadBody && typeof leadBody === 'object' ? leadBody : {};
    postObserve(
      kind || 'ghl_request',
      Object.assign(
        {
          source: shortText(b.source || '', 120),
          status: shortText(b.status || '', 80),
          type: shortText(b.type || '', 80),
          event_id: shortText(b.event_id || b.jobber_event_id || '', 160),
          phone_last4: String(b.phone_e164 || b.phone || '').replace(/\D/g, '').slice(-4),
          service_zip: shortText(b.service_zip || '', 20)
        },
        extra || {}
      )
    );
  }

  function observeException(kind, err, extra) {
    var e = err || {};
    postObserve(
      kind || 'client_exception',
      Object.assign(
        {
          error_name: shortText(e.name || '', 120),
          error_message: shortText(e.message || e.reason || e, 500),
          stack: shortText(e.stack || '', 1500)
        },
        extra || {}
      )
    );
  }

  function buildMetaObservePayload(channel, eventName, metaEventId, customData, userPlain, extra) {
    var plain = userPlain && typeof userPlain === 'object' ? userPlain : {};
    var custom = customData && typeof customData === 'object' ? customData : {};
    var b = state.bundle || {};
    var fbp = getCookie('_fbp');
    var fbc = getCookie('_fbc');
    return Object.assign(
      {
        channel: shortText(channel || '', 40),
        event_name: shortText(eventName || '', 80),
        event_id: shortText(metaEventId || '', 160),
        pixel_id: shortText(META_PIXEL_ID || '', 40),
        journey_event_id: shortText(journeyId() || '', 160),
        session_id: shortText(b.session_id || '', 160),
        service_zip: shortText(custom.service_zip || b.service_zip || '', 20),
        fbp_present: !!fbp,
        fbc_present: !!fbc,
        fbclid_present: !!b.fbclid,
        email_present: !!plain.email,
        phone_present: !!(plain.phone || plain.ph),
        first_name_present: !!(plain.fn || plain.first_name),
        last_name_present: !!(plain.ln || plain.last_name),
        zip_present: !!(plain.zp || custom.service_zip || b.service_zip),
        external_id_present: !!plain.external_id,
        value_present: custom.value != null,
        currency: shortText(custom.currency || '', 16),
        custom_data_keys: Object.keys(custom).slice(0, 20).join(','),
        test_event_code_present: !!(
          global.__NS_META_TEST_EVENT_CODE && String(global.__NS_META_TEST_EVENT_CODE).trim()
        )
      },
      extra || {}
    );
  }

  function observeMetaEvent(channel, eventName, metaEventId, customData, userPlain, extra) {
    postObserve(
      'meta_event',
      buildMetaObservePayload(channel, eventName, metaEventId, customData, userPlain, extra)
    );
  }

  function postLeadReceipt(payload) {
    if (!LEAD_RECEIPT_URL || !/^https:\/\//.test(LEAD_RECEIPT_URL)) {
      return;
    }
    var body = Object.assign(
      {
        journey_event_id: journeyId() || '',
        page_url: String(location.href || ''),
        referrer: String(document.referrer || ''),
        user_agent: typeof navigator !== 'undefined' ? String(navigator.userAgent || '') : '',
        ts: Date.now()
      },
      payload || {}
    );
    fetch(LEAD_RECEIPT_URL, {
      method: 'POST',
      mode: 'cors',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(body),
      keepalive: true
    }).catch(function () {});
  }

  function installGlobalObserveHandlers() {

    if (global.__NS_OBSERVE_INSTALLED) {
      return;
    }
    global.__NS_OBSERVE_INSTALLED = true;
    global.addEventListener('error', function (evt) {
      observeException('window_error', evt && evt.error ? evt.error : evt, {
        filename: shortText(evt && evt.filename ? evt.filename : '', 300),
        lineno: evt && evt.lineno ? evt.lineno : 0,
        colno: evt && evt.colno ? evt.colno : 0
      });
    });
    global.addEventListener('unhandledrejection', function (evt) {
      var reason = evt && evt.reason ? evt.reason : evt;
      observeException('unhandled_rejection', reason, {});
    });
  }

  /** metaEventId is the exact string sent as Meta event_id (CAPI) and Pixel eventID (browser). */
  function sendCapi(eventName, metaEventId, customData, userPlain) {
    var payload = {
      pixel_id: META_PIXEL_ID,
      event_name: eventName,
      event_id: metaEventId,
      event_source_url: String(location.href),
      custom_data: Object.assign(
        {
          journey_event_id: state.bundle.journey_event_id,
          session_id: state.bundle.session_id
        },
        customData || {}
      ),
      user_data_plain: userPlain || {},
      fbp: getCookie('_fbp'),
      fbc: getCookie('_fbc')
    };
    var capiTest = global.__NS_META_TEST_EVENT_CODE;
    if (capiTest != null && String(capiTest).trim()) {
      payload.test_event_code = String(capiTest).trim();
    }
    observeMetaEvent('capi_dispatch', eventName, metaEventId, payload.custom_data, userPlain, {
      capi_url: shortText(CAPI_URL || '', 200)
    });
    fetch(CAPI_URL, {
      method: 'POST',
      mode: 'cors',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(payload),
      keepalive: true
    })
      .then(function (res) {
        return res.text().then(function (txt) {
          if (!res.ok) {
            console.warn(
              '[NSTracking] CAPI failed',
              eventName,
              res.status,
              (txt || '').slice(0, 800)
            );
            observeMetaEvent('capi_result', eventName, metaEventId, payload.custom_data, userPlain, {
              http_status: res.status,
              response_body: shortText(txt || '', 500),
              ok: false
            });
          } else {
            observeMetaEvent('capi_result', eventName, metaEventId, payload.custom_data, userPlain, {
              http_status: res.status,
              response_body: shortText(txt || '', 500),
              ok: true
            });
            if (global.__NS_DEBUG_CAPI) {
              console.log('[NSTracking] CAPI ok', eventName, (txt || '').slice(0, 400));
            }
          }
        });
      })
      .catch(function (err) {
        console.warn(
          '[NSTracking] CAPI network error',
          eventName,
          err && err.message ? err.message : String(err)
        );
        observeMetaEvent('capi_network_error', eventName, metaEventId, payload.custom_data, userPlain, {
          error_message: shortText(err && err.message ? err.message : String(err), 500),
          ok: false
        });
      });
  }

  /** Optional 5th arg overrideMetaEventId: when set, used as Pixel eventID and CAPI event_id (Purchase = Stripe pi_*). */
  function trackStandard(eventName, slug, customData, userPlain, overrideMetaEventId) {
    var eventId =
      overrideMetaEventId != null && String(overrideMetaEventId).trim()
        ? String(overrideMetaEventId).trim()
        : dedupeId(slug);
    var opts = { eventID: eventId };
    observeMetaEvent('pixel', eventName, eventId, customData, userPlain, {
      pixel_track_type: 'standard'
    });
    if (typeof fbq !== 'undefined') {
      fbq('track', eventName, customData || {}, opts);
    }
    sendCapi(eventName, eventId, customData, userPlain);
  }

  function trackCustom(name, slug, customData, userPlain, overrideMetaEventId) {
    var eventId =
      overrideMetaEventId != null && String(overrideMetaEventId).trim()
        ? String(overrideMetaEventId).trim()
        : dedupeId(slug);
    var opts = { eventID: eventId };
    observeMetaEvent('pixel', name, eventId, customData, userPlain, {
      pixel_track_type: 'custom'
    });
    if (typeof fbq !== 'undefined') {
      fbq('trackCustom', name, customData || {}, opts);
    }
    sendCapi(name, eventId, customData, userPlain);
  }

  function standardLeadAlreadyFired() {
    try {
      return sessionStorage.getItem(SS_LEAD_FIRED) === '1';
    } catch (e) {
      return false;
    }
  }

  function markStandardLeadFired() {
    try {
      sessionStorage.setItem(SS_LEAD_FIRED, '1');
    } catch (e) {}
  }

  function appendParamsToUrl(urlStr) {
    var b = state.bundle;
    try {
      var u = new URL(urlStr, location.origin);
      if (b.journey_event_id && !u.searchParams.get('event_id')) {
        u.searchParams.set('event_id', b.journey_event_id);
      }
      ATTR_KEYS.forEach(function (k) {
        if (b[k] && !u.searchParams.get(k)) {
          u.searchParams.set(k, b[k]);
        }
      });
      if (b.session_id && !u.searchParams.get('session_id')) {
        u.searchParams.set('session_id', b.session_id);
      }
      return u.pathname + u.search + u.hash;
    } catch (e) {
      return urlStr;
    }
  }

  function hydrateHiddenFields(root) {
    var r = root || document;
    var set = function (id, val) {
      var el = r.getElementById ? r.getElementById(id) : document.getElementById(id);
      if (el) {
        el.value = val != null ? String(val) : '';
      }
    };
    var p = getAttributionPayload();
    set('ns-attr-event-id', p.journey_event_id);
    set('ns-attr-session-id', p.session_id);
    set('ns-attr-zip', p.service_zip);
    set('ns-attr-bins', p.bins);
    set('ns-attr-plan', p.plan_intent);
    ATTR_KEYS.forEach(function (k) {
      set('ns-attr-' + k.replace(/[^a-z0-9_]/gi, '_'), state.bundle[k] || '');
    });
  }

  function bootstrap() {
    if (state.bootstrapped) {
      return state.bundle;
    }
    installGlobalObserveHandlers();
    var fromUrl = captureFromUrl();
    var prev = readJsonLS(LS_ATTR) || {};
    state.bundle = mergeBundles(prev, fromUrl);
    ensureJourneyId(state.bundle);
    ensureSessionId(state.bundle);
    persistBundle();
    syncUrlParams();
    state.bootstrapped = true;

    if (BOOT === 'confirmation') {
      trackCustom(
        'ViewContent_Confirmation',
        'ViewContent_Confirmation',
        {
          content_name: 'thank_you_confirmation',
          journey_event_id: state.bundle.journey_event_id,
          session_id: state.bundle.session_id
        },
        {}
      );
      hydrateHiddenFields();
      return state.bundle;
    }

    var common = {
      content_name: 'Natural Sanitation Landing',
      journey_event_id: state.bundle.journey_event_id,
      session_id: state.bundle.session_id
    };
    ATTR_KEYS.forEach(function (k) {
      if (state.bundle[k]) {
        common[k] = state.bundle[k];
      }
    });

    trackStandard('PageView', 'PageView', common, {});
    trackStandard(
      'ViewContent',
      'ViewContent',
      Object.assign({ content_type: 'product_group' }, common),
      {}
    );

    hydrateHiddenFields();
    return state.bundle;
  }

  function fireClickCta(surface) {
    trackCustom(
      'ClickCTA',
      'ClickCTA',
      { surface: surface || 'unknown', journey_event_id: journeyId() },
      {}
    );
  }

  function fireSelectPlan(extra) {
    var plan = (extra && extra.plan) || '';
    var bins = (extra && extra.bins) != null ? extra.bins : '';
    trackCustom(
      'SelectPlan',
      'SelectPlan_' + plan + '_' + bins,
      Object.assign({ journey_event_id: journeyId() }, extra || {}),
      {}
    );
  }

  function fireLeadDetail(extra, userPlain, dedupeSlug) {
    trackCustom(
      'LeadDetail',
      dedupeSlug || 'LeadDetail',
      Object.assign({ journey_event_id: journeyId() }, extra || {}),
      userPlain || {}
    );
  }

  function fireOutOfAreaLead(extra, userPlain) {
    trackCustom(
      'OutOfAreaLead',
      'OutOfAreaLead',
      Object.assign({ waitlist: true, journey_event_id: journeyId() }, extra || {}),
      userPlain || {}
    );
  }

  function firePurchaseFailure(extra) {
    trackCustom(
      'PurchaseFailure',
      'PurchaseFailure_' + Date.now(),
      Object.assign({ journey_event_id: journeyId() }, extra || {}),
      {}
    );
  }

  function fireInitiateCheckoutStandard(customData, userPlain) {
    trackStandard('InitiateCheckout', 'InitiateCheckout', customData || {}, userPlain || {});
  }

  function firePurchaseStandard(customData, userPlain, piId) {
    if (!piId || String(piId).indexOf('pi_') !== 0) {
      return;
    }
    var slug = 'Purchase_' + piId;
    trackStandard('Purchase', slug, customData || {}, userPlain || {}, piId);
  }

  function fireLeadStandard(customData, userPlain) {
    trackStandard('Lead', 'Lead', customData || {}, userPlain || {});
  }

  global.NSTracking = {
    bootstrap: bootstrap,
    journeyId: journeyId,
    dedupeId: dedupeId,
    syncUrl: syncUrlParams,
    getAttributionPayload: getAttributionPayload,
    updateContext: updateContext,
    appendParamsToUrl: appendParamsToUrl,
    hydrateHiddenFields: hydrateHiddenFields,
    trackStandard: trackStandard,
    trackCustom: trackCustom,
    fireClickCta: fireClickCta,
    fireSelectPlan: fireSelectPlan,
    fireLeadDetail: fireLeadDetail,
    fireOutOfAreaLead: fireOutOfAreaLead,
    firePurchaseFailure: firePurchaseFailure,
    fireInitiateCheckoutStandard: fireInitiateCheckoutStandard,
    firePurchaseStandard: firePurchaseStandard,
    fireLeadStandard: fireLeadStandard,
    standardLeadAlreadyFired: standardLeadAlreadyFired,
    markStandardLeadFired: markStandardLeadFired,
    postObserve: postObserve,
    observeLeadRequest: observeLeadRequest,
    observeException: observeException,
    postLeadReceipt: postLeadReceipt,
    META_PIXEL_ID: META_PIXEL_ID,
    CAPI_URL: CAPI_URL,
    OBSERVE_URL: OBSERVE_URL,
    LEAD_RECEIPT_URL: LEAD_RECEIPT_URL
  };

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', function () {
      try {
        bootstrap();
      } catch (e) {}
    });
  } else {
    try {
      bootstrap();
    } catch (e3) {}
  }
})(typeof window !== 'undefined' ? window : this);
