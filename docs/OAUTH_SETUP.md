# OAuth setup

You need one Google OAuth client (covers GA4 + Search Console + Google Ads) and
one Meta app (covers Meta Ads + Instagram Insights). Both flows are standard
authorization-code OAuth 2.0; the app stores refresh/access tokens encrypted at
rest and never exposes them to the frontend.

Redirect URIs below assume `PUBLIC_BASE_URL=http://localhost:8000`. Change to
match your deployment and keep `.env` in sync.

---

## Google  (`GOOGLE_CLIENT_ID`, `GOOGLE_CLIENT_SECRET`, `GOOGLE_REDIRECT_URI`)

1. **Create / pick a project** — <https://console.cloud.google.com/>.
2. **Enable the APIs** the connectors call (APIs & Services → Library):
   - *Google Analytics Data API* and *Google Analytics Admin API* (GA4)
   - *Google Search Console API*
   - *Google Ads API* (only if you will use the Ads connector)
3. **OAuth consent screen** — External. Add scopes:
   - `.../auth/analytics.readonly`
   - `.../auth/webmasters.readonly`
   - `.../auth/adwords`
   - `openid`, `email`, `profile`
   While the app is in *Testing*, add each Google account you will connect as a
   **Test user** (otherwise consent is refused).
4. **Credentials → Create credentials → OAuth client ID → Web application.**
   Authorised redirect URI (exact match):
   ```
   http://localhost:8000/api/v1/oauth/google/callback
   ```
5. Put the client id/secret in `.env`:
   ```
   GOOGLE_CLIENT_ID=...
   GOOGLE_CLIENT_SECRET=...
   GOOGLE_REDIRECT_URI=http://localhost:8000/api/v1/oauth/google/callback
   ```

The connector requests **only the scope(s) it needs** — connecting Search Console
never asks for Analytics or Ads access. Reconnecting a second Google service
adds its scope incrementally to the same identity (`include_granted_scopes`).

### Google Ads — extra requirement

Google Ads also needs a **developer token**:

1. In a Google Ads **manager (MCC)** account → Tools → API Center → apply for a
   token. A token in *Test* status only reaches test accounts; production access
   requires Google's approval.
2. ```
   GOOGLE_ADS_DEVELOPER_TOKEN=...
   GOOGLE_ADS_API_VERSION=v25          # bump when Google sunsets a version
   GOOGLE_ADS_LOGIN_CUSTOMER_ID=1234567890   # digits only; the MCC, if the target account sits under one
   ```
Without the token the connector reports `INVALID_CONFIGURATION` with the exact
fix, rather than a raw 403.

---

## Meta  (`META_APP_ID`, `META_APP_SECRET`, `META_REDIRECT_URI`)

1. **Create an app** — <https://developers.facebook.com/apps/> → type *Business*.
2. Add products: **Facebook Login**, **Marketing API** (for Meta Ads).
3. **Facebook Login → Settings → Valid OAuth Redirect URIs**:
   ```
   http://localhost:8000/api/v1/oauth/meta/callback
   ```
4. `.env`:
   ```
   META_APP_ID=...
   META_APP_SECRET=...
   META_REDIRECT_URI=http://localhost:8000/api/v1/oauth/meta/callback
   META_API_VERSION=v26.0
   ```
5. **Permissions** requested per connector:
   - Meta Ads — `ads_read`, `business_management`
   - Instagram Insights — `instagram_basic`, `instagram_manage_insights`,
     `pages_show_list`, `pages_read_engagement`

### Meta access levels

- In **Development mode** the app works for users with a **role** on the app
  (admin/developer/tester). This is enough to build and test end-to-end.
- To connect accounts owned by anyone else you must submit the permissions above
  for **App Review** and switch the app to **Live**.
- **Meta issues no refresh token.** The code exchange is upgraded to a
  long-lived user token (~60 days); when it expires the connection moves to
  `needs_reauth` and a human must click **Reconnect**. This is modelled
  explicitly — the app never attempts a doomed refresh.

### Instagram specifics

Instagram Insights requires an Instagram **Business or Creator** account linked
to a Facebook Page. Discovery lists Pages without a linked IG account as
non-selectable with the reason; a personal IG account yields a clear
`NOT_SUPPORTED` at connection check rather than an obscure failure. Account-level
insights serve roughly the last 30 days; audience/demographic metrics are not
implemented in v1 (they are version- and eligibility-dependent).

---

## Encryption key (required)

OAuth tokens are encrypted at rest with Fernet. Generate and set:

```bash
python -c "from cryptography.fernet import Fernet;print(Fernet.generate_key().decode())"
```

```
ENCRYPTION_KEY=<the generated key>
```

Rotation: put the **new** key first, keep old keys after it, comma-separated.
Existing rows stay readable and re-encrypt on next write. Outside
`ENVIRONMENT=development` the app refuses to start without a real key.

---

## Verifying the flow

1. `uvicorn app.main:app --reload`, open `http://localhost:8000/`.
2. Paste the dev bearer token (printed to the log on first start).
3. Click **Connect** on an integration whose credentials you configured → you are
   sent to the real provider consent screen → back to the app as
   "Connected as you@example.com".
4. **Discover resources**, pick a property/account, set a backfill window and
   interval, **Create connection** → watch the live progress bar → **Data**.
