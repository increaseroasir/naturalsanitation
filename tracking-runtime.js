/**
 * Natural Sanitation — shared browser attribution + Meta Pixel/CAPI bridge.
 * - One persistent journey id (localStorage ns_funnel_event_id) for CRM / Hyros stitching.
 * - Meta browser + server dedupe: each firing uses eventID = journeyId + ':' + slug (slug unique per event type).
 * - CAPI is sent only to your Worker (META_CAPI_ACCESS_TOKEN stays on the server).
 *
 * Config (optional on window before this script):
 *   __NS_META_PIXEL_ID, __NS_CAPI_URL
 *   __NS_META_TEST_EVENT_CODE — e.g. TEST28089; forwarded to the Worker as test_event_code (Meta Test Events stream).
 *     Remove or empty for production once CAPI is verified.
 */
(function (global) {
  'use strict';

  var META_PIXEL_ID = global.__NS_META_PIXEL_ID || '499919262310418';
  var CAPI_URL =
    global.__NS_CAPI_URL ||
    'https://ns-payment.increase-roas.workers.dev/meta-capi';

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

  function sendCapi(eventName, slug, customData, userPlain) {
    var payload = {
      pixel_id: META_PIXEL_ID,
      event_name: eventName,
      event_id: dedupeId(slug),
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
    fetch(CAPI_URL, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(payload),
      keepalive: true
    }).catch(function () {});
  }

  function trackStandard(eventName, slug, customData, userPlain) {
    var opts = { eventID: dedupeId(slug) };
    if (typeof fbq !== 'undefined') {
      fbq('track', eventName, customData || {}, opts);
    }
    sendCapi(eventName, slug, customData, userPlain);
  }

  function trackCustom(name, slug, customData, userPlain) {
    var opts = { eventID: dedupeId(slug) };
    if (typeof fbq !== 'undefined') {
      fbq('trackCustom', name, customData || {}, opts);
    }
    sendCapi(name, slug, customData, userPlain);
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
    trackStandard('Purchase', slug, customData || {}, userPlain || {});
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
    META_PIXEL_ID: META_PIXEL_ID,
    CAPI_URL: CAPI_URL
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
