"""Quick test for the insights endpoint."""
import json
from app import app

client = app.test_client()

payload = {
    'summary': {
        'global': {
            'n': 100,
            'ev_ebitda': {'median': 8.5, 'p25': 6.0, 'p75': 12.0, 'mean': 9.5, 'n': 100},
            'ev_revenue': {'median': 2.0, 'p25': 1.2, 'p75': 3.0, 'n': 100},
            'fcf_revenue': {'median': 0.08, 'p25': 0.04, 'p75': 0.12, 'n': 80},
            'fcf_ebitda': {'median': 0.5, 'p25': 0.3, 'p75': 0.7, 'n': 80}
        },
        'brasil': {
            'n': 15,
            'ev_ebitda': {'median': 6.2, 'p25': 4.5, 'p75': 8.0, 'n': 15},
            'ev_revenue': {'median': 1.5, 'p25': 1.0, 'p75': 2.0, 'n': 15},
            'fcf_revenue': {'median': 0.06, 'p25': 0.03, 'p75': 0.1, 'n': 12},
            'fcf_ebitda': {'median': 0.4, 'p25': 0.25, 'p75': 0.6, 'n': 12}
        },
        'latam': {
            'n': 25,
            'ev_ebitda': {'median': 7.0, 'p25': 5.0, 'p75': 9.0, 'n': 25},
            'ev_revenue': {'median': 1.7, 'p25': 1.1, 'p75': 2.3, 'n': 25},
            'fcf_revenue': {'median': 0.07, 'p25': 0.035, 'p75': 0.11, 'n': 20},
            'fcf_ebitda': {'median': 0.45, 'p25': 0.28, 'p75': 0.65, 'n': 20}
        }
    },
    'by_industry': [
        {'label': 'Water Utilities', 'n': 10, 'ev_ebitda': {'median': 35.0, 'p25': 25.0, 'p75': 45.0}},
        {'label': 'Electric Utilities', 'n': 30, 'ev_ebitda': {'median': 9.0, 'p25': 7.0, 'p75': 11.0}},
        {'label': 'Gas Utilities', 'n': 8, 'ev_ebitda': {'median': 7.5, 'p25': 5.0, 'p75': 10.0, 'n': 8}}
    ],
    'by_geography': [
        {'label': 'Asia', 'n': 20, 'ev_ebitda': {'median': 12.0}},
        {'label': 'Europe', 'n': 15, 'ev_ebitda': {'median': 7.0}}
    ],
    'companies': [
        {'ticker': 'AWK', 'company_name': 'American Water Works', 'country': 'US', 'ev_ebitda': 45.0},
        {'ticker': 'WTRG', 'company_name': 'Essential Utilities', 'country': 'US', 'ev_ebitda': 38.0},
    ] + [{'ticker': f'C{i}', 'ev_ebitda': 8.0 + (i * 0.1)} for i in range(20)],
    'evolution': [
        {'year': 2022, 'global': {'ev_ebitda': {'median': 10.5}, 'ev_revenue': {'median': 2.5}, 'fcf_revenue': {'median': 0.1}, 'fcf_ebitda': {'median': 0.55}}},
        {'year': 2023, 'global': {'ev_ebitda': {'median': 9.0}, 'ev_revenue': {'median': 2.2}, 'fcf_revenue': {'median': 0.09}, 'fcf_ebitda': {'median': 0.5}}},
        {'year': 2024, 'global': {'ev_ebitda': {'median': 8.0}, 'ev_revenue': {'median': 2.0}, 'fcf_revenue': {'median': 0.08}, 'fcf_ebitda': {'median': 0.48}}},
        {'year': 2025, 'global': {'ev_ebitda': {'median': 8.5}, 'ev_revenue': {'median': 2.0}, 'fcf_revenue': {'median': 0.08}, 'fcf_ebitda': {'median': 0.5}}}
    ],
    'metadata': {'sector': 'Utilities', 'fiscal_year': '2025'}
}

r = client.post('/api/estudoanloc/insights',
    data=json.dumps(payload),
    content_type='application/json')

data = json.loads(r.data)
print(f"Success: {data['success']}")
print(f"Total insights: {data['total']}")
print(f"Mode: {data['metadata']['mode']}")
print()
for ins in data['insights']:
    sev = ins['severity'].upper()
    print(f"  [{sev:7}] [{ins['category']:10}] {ins['title']}")
    print(f"           {ins['text'][:120]}")
    print()
