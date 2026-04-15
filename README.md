# of-vivant-cin7-hubspot

**OrderFloz Implementation: Vivant Skincare**

Cin7 Omni → HubSpot CRM order sync for Vivant Skincare wholesale operations.

## Integration Details

| Field | Value |
|-------|-------|
| Client | Vivant Skincare |
| Source | Cin7 Omni |
| Destination | HubSpot CRM |
| Data Synced | Wholesale orders → Closed Won deals |

## Features

- 🔄 Fetch orders by dispatched date range
- 🏢 Wholesale segment filtering
- ✅ Status filtering (Approved, Dispatched, Voided)
- ⚠️ Review queue for internal/employee orders
- 🚀 One-click push to HubSpot
- 🎨 White-label branding support

## Deployment

Hosted on Streamlit Cloud at: `https://vivant-cin7-hspot.streamlit.app`

## Configuration

### Streamlit Secrets (TOML format)

```toml
# API Credentials
CIN7_USERNAME = "your_username"
CIN7_API_KEY = "your_api_key"
HUBSPOT_API_KEY = "your_hubspot_token"

# Branding (optional)
[branding]
company_name = "Vivant Skincare"
logo_url = "https://your-logo-url.com/logo.png"
primary_color = "#1a5276"
support_email = "support@vivantskincare.com"
powered_by = true
```

### Branding Options

| Setting | Description | Default |
|---------|-------------|---------|
| `company_name` | Displayed in header & footer | "OrderFloz" |
| `logo_url` | URL to company logo (80px width) | None |
| `primary_color` | Main brand color (hex) | "#1a5276" |
| `support_email` | Shown in footer | "support@orderfloz.com" |
| `powered_by` | Show "Powered by OrderFloz" | true |

## Local Development

```bash
pip install -r requirements.txt
streamlit run app.py
```

## License

Proprietary — © Monemtech LLC

---

**OrderFloz** by [Monemtech](https://monemtech.com)
