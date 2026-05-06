# Natural Sanitation — AI Agent Handoff Prompt

> Copy and paste the block below as the opening message in a new session to give the agent full context.

---

## Handoff Prompt

You are continuing work on the **Natural Sanitation** performance tracking and funnel infrastructure project. Here is the full context you need to pick up immediately.

---

### Project Overview

Natural Sanitation is a garbage can cleaning service running paid Meta ads to a funnel at `https://naturalsanitationsignup.com`. The funnel captures leads (opt-ins), routes them through an online checkout (Stripe) or a phone close (Jobber), and logs all attribution data to Hyros for ROAS reporting. A custom performance dashboard at `/dashboard` (password: `ns2026dash`) shows live metrics pulled from the Cloudflare Worker.

---

### Key Infrastructure

| Component | Details |
|---|---|
| **Funnel domain** | `https://naturalsanitationsignup.com` (Cloudflare Pages) |
| **GitHub repo** | `https://github.com/increaseroasir/naturalsanitation.git` (branch: `main`) |
| **Cloudflare Worker** | `ns-payment` at `https://ns-payment.increase-roas.workers.dev` |
| **Dashboard** | `https://naturalsanitationsignup.com/dashboard` — password: `ns2026dash` |
| **Meta Ad Account** | `act_2659531691011918` — timezone: `America/Detroit` |
| **Meta Pixel** | `499919262310418` |
| **Hyros** | Attribution platform — all sales must be logged here with correct tags |
| **GoHighLevel (GHL)** | CRM — contacts tagged `purchasefunnellead` for funnel attribution |
| **Stripe** | Online checkout payments |
| **Jobber** | Phone close invoicing — paid invoices trigger the Hyros sale log |
| **Zapier** | Automation bridge between Jobber, GHL, and Hyros |
| **Google Sheets** | Audit sheet ID `1Fr_rsnz7MvzR7LsYmTANT_ReVhkW8V-tEL_jvdnKCi4` |
| **Apps Script webhook** | `https://script.google.com/macros/s/AKfycbykVEHa9Cs5v8cWapRNsa_P1kiFOnB4fUbf5s3fHpotnsLNOBPgarV8eDt31LkdUr9a/exec` |

---

### Worker Endpoints

| Endpoint | Purpose |
|---|---|
| `POST /create-payment-intent` | Creates Stripe PaymentIntent |
| `POST /confirm-purchase` | Logs Stripe purchase to Hyros |
| `POST /ghl-lead` | Upserts contact in GoHighLevel |
| `POST /meta-capi` | Fires server-side Meta Conversions API event |
| `POST /jobber-sale` | Receives Jobber invoice data from Zapier, looks up GHL tags, fires Hyros with correct attribution (`$phone-close-ad-lead` for funnel leads, `$website-organic` for organic) |
| `POST /stripe-webhook` | Handles Stripe subscription renewals → Hyros recurring order |
| `POST /client-observe` | Browser observability sink |
| `POST /lead-receipt` | Backup lead logging to Google Sheets |
| `GET /dashboard-api?period=` | Returns live dashboard metrics (today / 3d / 7d / mtd). Requires header `X-Dashboard-Key: ns2026dash` |

---

### Hyros Attribution Tags

| Tag | Meaning |
|---|---|
| `$phone-close-ad-lead` | Phone close from a funnel/ad lead (has `purchasefunnellead` in GHL) |
| `$funnel-self-purchase` | Online checkout purchase via the funnel |
| `$sent-link-purchase` | Purchase via a sent payment link |
| `$website-organic` | Organic purchase (no funnel tag in GHL) |
| `$subscription-renewal` | Stripe subscription renewal |

---

### Zapier Automations

| Zap Name | Trigger | Action | Status |
|---|---|---|---|
| `ns-ghl-contact-to-hyros-lead` | GHL new contact | Hyros Create Lead | Active |
| `ns-jobber-sale-to-hyros` (HYROS Zapier Jobber) | Jobber **Quote Approved** | POST to Worker `/jobber-sale` | **⚠️ Needs fix — see below** |

---

### Open Issues as of May 6, 2026

#### Issue 1: Dashboard CORS Error — FIXED ✅
The dashboard was showing "Error loading data" on the Today period due to a CORS preflight failure. The browser `fetch` was sending a `Cache-Control` request header that was not in the Worker's `Access-Control-Allow-Headers` list. Fixed by:
- Adding `Cache-Control` to the Worker's CORS allowed headers
- Removing the redundant `Cache-Control` header from the dashboard fetch call
- Both changes deployed to production on May 6, 2026 (commit `7485bd0`)

#### Issue 2: Phone Closes Not Tracking in Hyros — NEEDS FIX ⚠️
**Symptom:** Phone close sales (Jobber invoices) are not appearing in Hyros or the dashboard.

**Root cause:** The Zapier Zap "HYROS Zapier Jobber" is configured with the wrong trigger — it fires on **"Quote Approved"** instead of **"Invoice Updated/Paid"**. When a phone close customer pays, Jobber creates a *paid invoice*, not a quote approval. The Zap never fires for actual payments.

**Confirmed case:** Shawn Henley (phone: 313-283-0394) was closed today. His Jobber quote was approved (triggering the Zap test), but his *payment* was not captured. No Hyros sale record exists for him.

**Fix required in Zapier:**
1. Open Zap: **HYROS Zapier Jobber**
2. Change Step 1 trigger from `Quote Approved` → **`Invoice Updated`**
3. Update Step 2 filter: `Invoice Status` exactly matches **`paid`**
4. Step 3 (Webhook POST to Worker `/jobber-sale`) stays exactly the same

**Webhook POST payload mapping (Step 3 — do not change):**

| Field | Jobber source |
|---|---|
| `email` | Client Primary Email Address |
| `amount` | Invoice Total |
| `invoice_number` | Invoice Number |
| `first_name` | Client First Name |
| `last_name` | Client Last Name |
| `phone` | Client Primary Phone Number |
| `invoice_date` | Invoice Created At |

**For Shawn Henley specifically:** His sale needs to be manually pushed to Hyros. Fire the Worker endpoint directly:
```bash
curl -X POST https://ns-payment.increase-roas.workers.dev/jobber-sale \
  -H "Content-Type: application/json" \
  -d '{
    "first_name": "Shawn",
    "last_name": "Henley",
    "phone": "3132830394",
    "email": "",
    "amount": <INSERT_AMOUNT>,
    "invoice_number": "<INSERT_INVOICE_NUMBER>",
    "invoice_date": "2026-05-06"
  }'
```

---

### Dashboard Metrics Reference

The dashboard at `/dashboard` tracks the following KPIs with targets:

| Metric | Target |
|---|---|
| Opt-in rate (form fills ÷ visitors) | 25–30% |
| Online conversion rate (buyers ÷ opt-ins) | 12–15% |
| Phone contact rate | 60–70% |
| Phone close rate | 30–40% |
| Blended close rate | Current baseline: 12.3% |
| Average Order Value | ~$150 |

---

### Secrets (stored in Cloudflare Worker — never expose in browser)

`STRIPE_SECRET_KEY`, `META_CAPI_ACCESS_TOKEN`, `GHL_API_TOKEN`, `GHL_LOCATION_ID`, `GHL_FUNNEL_EVENT_FIELD_ID`, `META_TEST_EVENT_CODE`, `GOOGLE_SHEET_WEBHOOK_URL`, `HYROS_API_KEY`, `META_ADS_TOKEN`, `STRIPE_WEBHOOK_SECRET`

---

### Important Notes

- Do not assume `Clicks (all)` from Meta equals funnel visits. Use **Website Landing Page Views** for that comparison.
- `_fbp`, `_fbc`, and `external_id` tracking were previously repaired and are working correctly.
- The Google Sheets audit tab (`Meta Events`) is an important verification layer for Meta CAPI events.
- The GitHub repo is the source of truth. Always `git pull` before making changes and `git push` after.
- Cloudflare Pages auto-deploys from the `main` branch. The Worker must be deployed separately with `wrangler deploy`.
