# Dashboard & Zapier Fix Handoff

**Date:** May 6, 2026
**Author:** Manus AI

## 1. Dashboard "Error loading data" Fix

### The Issue
The Natural Sanitation dashboard was showing "Error loading data" specifically when loading the "Today" period, while other periods like MTD worked fine. 

### Root Cause
The issue was caused by a CORS (Cross-Origin Resource Sharing) preflight failure. The browser's `fetch` request for the dashboard data was sending a custom `Cache-Control` header. However, the Cloudflare Worker's CORS configuration did not include `Cache-Control` in its `Access-Control-Allow-Headers` list. This caused the browser to block the request entirely, resulting in a silent failure that triggered the error catch block in the dashboard UI.

### The Fix
1. **Cloudflare Worker:** Added `Cache-Control` to the allowed CORS headers in `worker.js`.
2. **Dashboard UI:** Removed the redundant `Cache-Control` header from the `fetch` request in `dashboard.html` (since the `cache: 'no-store'` fetch option already handles cache prevention natively).
3. Both fixes have been deployed to production. The dashboard now correctly loads and displays today's data without error.

---

## 2. Missing Phone Close (Shawn Henley)

### The Issue
Shawn Henley (Phone: 313-283-0394) was closed over the phone today, but his sale did not appear in the dashboard's "Phone Closes" metric or in Hyros.

### Root Cause
The issue lies in the Zapier configuration for the **Jobber → Hyros** automation (`ns-jobber-sale-to-hyros`).

1. **Incorrect Trigger Event:** The Zap is currently configured to trigger on **"Quote Approved"**. 
2. **Missing Payment Trigger:** When Shawn paid, a Jobber *invoice* was marked as paid, but because the Zap only listens for quote approvals, it never fired for the actual payment event.
3. As a result, the Cloudflare Worker's `/jobber-sale` endpoint was never called, and the sale was never pushed to Hyros with the `$phone-close-ad-lead` tag.

### Required Action (Zapier Fix)
To fix this permanently so phone closes track correctly:

1. Open the Zap **"HYROS Zapier Jobber"** in Zapier.
2. Change the Trigger (Step 1) from `Quote Approved` to **`Invoice Updated`** (or `Invoice Paid` depending on Jobber's exact event names in Zapier).
3. Update the Filter (Step 2) to only continue if the `Invoice Status` exactly matches **`paid`**.
4. Keep the Webhook POST (Step 3) exactly as it is.

### Immediate Next Steps for Shawn
Because the Zap didn't fire for his payment, Shawn's sale needs to be manually pushed to Hyros to reflect in today's dashboard numbers. 

To do this, please provide:
1. **Shawn's full name** (Confirmed as Shawn Henley)
2. **The exact sale amount** he paid today

Once provided, I can manually fire the webhook to log his sale correctly in Hyros and the dashboard.
