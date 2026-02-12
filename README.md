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

## Deployment

Hosted on Railway at: `[deployment URL]`

## Configuration

Environment variables (set in Railway):
- `CIN7_USERNAME` — Cin7 API username
- `CIN7_API_KEY` — Cin7 API key  
- `HUBSPOT_API_KEY` — HubSpot Private App token

## Local Development

```bash
pip install -r requirements.txt
streamlit run app.py
```

## License

Proprietary — © Monemtech LLC

---

**OrderFloz** by [Monemtech](https://monemtech.com)
