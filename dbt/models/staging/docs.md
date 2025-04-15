{% docs claim_type_codes %}

National Claims History (NCH) Claim Type Codes are used by CMS to categorize different types of Medicare claims. Each claim is assigned a specific code that indicates the type of service provided.

| Code | Description                   | Category | Details                                           |
|------|-------------------------------|----------|---------------------------------------------------|
| 10   | Home Health Agency           | HHA       | Claims for home-based healthcare services         |
| 20   | Non Swing Bed SNF            | SNF       | Skilled nursing facility claims (non-swing bed)   |
| 30   | Swing Bed SNF                | SNF       | Skilled nursing facility claims (swing bed)       |
| 40   | Outpatient                   | OUT       | Hospital outpatient services                      |
| 50   | Hospice                      | HSP       | End-of-life care services                         |
| 60   | Inpatient                    | INP       | Hospital inpatient stays and services             |
| 61   | Inpatient Full-Encounter     | INP       | Complete hospital inpatient encounters            |
| 71   | Carrier Non-DMERC            | CAR       | Professional/physician services (non-DMERC)       |
| 72   | Carrier DMERC                | DME       | Durable Medical Equipment claims                  |

**Usage in Analysis:**
- Use these codes to segment claims by service type
- Group codes by category (e.g., INP, OUT, SNF) for high-level analysis
- Consider both codes 60 and 61 when analyzing inpatient services
- Separate analysis may be needed for swing bed vs non-swing bed SNF claims

**Data Quality:**
- All claims should have a valid NCH claim type code
- Codes outside this list should be flagged for investigation
- The claim type should align with other claim attributes (e.g., provider type)


{% enddocs %}