"""
PE Dashboard sync script.
Queries Snowflake and updates RAW_DATA in index.html.
"""
import os, re, json
import snowflake.connector

ACCOUNT   = os.environ["SNOWFLAKE_ACCOUNT"]
USER      = os.environ["SNOWFLAKE_USER"]
PASSWORD  = os.environ["SNOWFLAKE_PASSWORD"]
WAREHOUSE = "CURSOR_MCP_WH"
ROLE      = "BASIC_USER_ROLE"

QUERY = """
SELECT
    CASE pe_id
        WHEN '001a700000QVNpYAAX' THEN 'HIG Capital'
        WHEN '001a7000008fXWrAAM' THEN 'Vista Equity Partners'
        WHEN '001a700000RaIN3AAN' THEN 'Nordic Capital'
        WHEN '001a700000DVEQfAAP' THEN 'HgCapital LLP'
        WHEN '001Hs00003VUZ0GIAX' THEN 'Permira Advisers LLC'
        WHEN '001a700000FNFfNAAX' THEN 'General Atlantic'
        WHEN '001a700000Q8INfAAN' THEN 'TPG'
        WHEN '001Hs00003XjNQIIA3' THEN 'TCV'
        WHEN '001Hs00003Uc7eiIAB' THEN 'Goldman Sachs'
        WHEN '001a700000CKV5vAAH' THEN 'KKR'
        WHEN '001Hs00003WTfZ5IAL' THEN 'Thoma Bravo'
        WHEN '001a700000DSqcKAAT' THEN 'Advent'
        WHEN '001a700000OzLbTAAV' THEN 'CVC'
        WHEN '001a700000DuSpGAAV' THEN 'Cove Hill Partners'
        WHEN '001a700000E80WtAAJ' THEN 'Growth Factors'
        WHEN '001a700000W6l3XAAR' THEN 'Parthenon Capital'
        WHEN '001a700000BBMfGAAX' THEN 'Khosla Ventures'
    END AS fund,
    COUNT(DISTINCT portco_id)                                                                                    AS total_portcos,
    COUNT(DISTINCT CASE WHEN portco.ACCOUNT_FIT_C IN ('Good Fit','Best Fit') THEN portco_id END)                AS enterprise_viable,
    COUNT(DISTINCT CASE WHEN dbt.TYPE='Customer' AND ctotal.contract_total IS NOT NULL THEN portco_id END)      AS enterprise_won,
    COUNT(DISTINCT CASE WHEN dbt.TYPE = 'Customer' THEN portco_id END)                                          AS closed_won,
    SUM(DISTINCT CASE WHEN dbt.TYPE = 'Customer' THEN ROUND(dbt.ARR) ELSE 0 END)                               AS won_arr,
    COUNT(DISTINCT CASE WHEN opp.ACCOUNT_ID IS NOT NULL AND NVL(dbt.TYPE,'x') != 'Customer' THEN portco_id END) AS open_pipe_count,
    COALESCE(SUM(CASE WHEN opp.ACCOUNT_ID IS NOT NULL AND NVL(dbt.TYPE,'x') != 'Customer' THEN opp.opp_amount END), 0) AS open_pipe_amount,
    COUNT(DISTINCT CASE WHEN dql.ACCOUNT_ID IS NOT NULL THEN portco_id END)                                     AS dq_closed_lost,
    COALESCE(SUM(DISTINCT CASE WHEN dbt.TYPE='Customer' AND ctotal.contract_total IS NOT NULL THEN ctotal.contract_total ELSE 0 END), 0)
      + COALESCE(SUM(CASE WHEN dbt.TYPE='Customer' AND ctotal.contract_total IS NULL THEN stotal.stripe_total END), 0) AS total_spend
FROM (
    SELECT pe.ID AS pe_id, inv.PORTFOLIO_COMPANY_C AS portco_id
    FROM FIVETRAN.SALESFORCE.ACCOUNT pe
    JOIN FIVETRAN.SALESFORCE.INVESTMENTS_C inv ON inv.INVESTMENT_FIRM_C = pe.ID AND inv.IS_DELETED = FALSE
    WHERE pe.ID IN (
        '001a700000DVEQfAAP','001a7000008fXWrAAM','001Hs00003VUZ0GIAX','001a700000QVNpYAAX',
        '001a700000FNFfNAAX','001a700000Q8INfAAN','001Hs00003XjNQIIA3','001Hs00003Uc7eiIAB',
        '001a700000CKV5vAAH','001Hs00003WTfZ5IAL','001a700000DSqcKAAT','001a700000OzLbTAAV',
        '001a700000DuSpGAAV','001a700000RaIN3AAN','001a700000E80WtAAJ','001a700000W6l3XAAR','001a700000BBMfGAAX')
    UNION ALL
    SELECT pe.ID AS pe_id, a.ID AS portco_id
    FROM FIVETRAN.SALESFORCE.ACCOUNT pe
    JOIN FIVETRAN.SALESFORCE.ACCOUNT a ON a.PRIMARY_PE_PARTNER_C = pe.ID AND a.IS_DELETED = FALSE
    WHERE pe.ID IN (
        '001a700000DVEQfAAP','001a7000008fXWrAAM','001Hs00003VUZ0GIAX','001a700000QVNpYAAX',
        '001a700000FNFfNAAX','001a700000Q8INfAAN','001Hs00003XjNQIIA3','001Hs00003Uc7eiIAB',
        '001a700000CKV5vAAH','001Hs00003WTfZ5IAL','001a700000DSqcKAAT','001a700000OzLbTAAV',
        '001a700000DuSpGAAV','001a700000RaIN3AAN','001a700000E80WtAAJ','001a700000W6l3XAAR','001a700000BBMfGAAX')
      AND a.TYPE IN ('Prospect','Customer','Partner')
) src
JOIN FIVETRAN.SALESFORCE.ACCOUNT portco ON portco.ID = src.portco_id AND portco.IS_DELETED = FALSE
LEFT JOIN DBT.PROD.ACCOUNTS dbt ON dbt.ACCOUNT_ID = portco.ID
LEFT JOIN (SELECT ACCOUNT_ID, SUM(TOTAL_CONTRACT_VALUE) AS contract_total FROM DBT.PROD.CONTRACTS GROUP BY 1) ctotal ON ctotal.ACCOUNT_ID = portco.ID
LEFT JOIN (SELECT acc.ACCOUNT_ID, SUM(si.AMOUNT_PAID) AS stripe_total FROM DBT.PROD.ACCOUNTS acc JOIN DBT.PROD.STRIPE_INVOICES si ON si.CUSTOMER_ID = acc.CUSTOMER_ID WHERE si.STATUS='paid' AND si.AMOUNT_PAID > 0 GROUP BY 1) stotal ON stotal.ACCOUNT_ID = portco.ID
LEFT JOIN (SELECT ACCOUNT_ID, SUM(AMOUNT) AS opp_amount FROM FIVETRAN.SALESFORCE.OPPORTUNITY WHERE IS_DELETED=FALSE AND IS_WON=FALSE AND IS_CLOSED=FALSE AND STAGE_NAME NOT IN ('Closed Lost','Disqualified') GROUP BY 1) opp ON opp.ACCOUNT_ID = portco.ID
LEFT JOIN (SELECT DISTINCT ACCOUNT_ID FROM FIVETRAN.SALESFORCE.OPPORTUNITY WHERE IS_DELETED=FALSE AND STAGE_NAME IN ('Closed Lost','Disqualified')) dql ON dql.ACCOUNT_ID = portco.ID
GROUP BY pe_id
ORDER BY closed_won DESC
"""

STAGE_MAP = {
    "HgCapital LLP":        "Portco Playbook Deployment",
    "Vista Equity Partners": "Activation",
    "Permira Advisers LLC":  "Activation",
    "HIG Capital":           "Activation",
}

ZERO_FUNDS = ["Growth Factors", "Parthenon Capital", "Khosla Ventures"]

def fmt(v):
    return int(round(float(v))) if v is not None else 0

def main():
    conn = snowflake.connector.connect(
        account=ACCOUNT, user=USER, password=PASSWORD,
        warehouse=WAREHOUSE, role=ROLE,
        database="FIVETRAN", schema="SALESFORCE",
    )
    cur = conn.cursor()
    cur.execute(QUERY)
    rows = cur.fetchall()
    cols = [d[0].lower() for d in cur.description]
    cur.close(); conn.close()

    results = {r[0]: dict(zip(cols, r)) for r in rows}

    js_rows = []
    all_funds = list(results.keys()) + [f for f in ZERO_FUNDS if f not in results]
    for fund in all_funds:
        if fund in results:
            r = results[fund]
            stage = STAGE_MAP.get(fund, "Portco Recruiting")
            js_rows.append(
                f'  {{fund:"{fund}", stage:"{stage}", '
                f'total_portcos:{fmt(r["total_portcos"])}, '
                f'enterprise_viable:{fmt(r["enterprise_viable"])}, '
                f'enterprise_won:{fmt(r["enterprise_won"])}, '
                f'open_pipe_count:{fmt(r["open_pipe_count"])}, '
                f'open_pipe_amount:{fmt(r["open_pipe_amount"])}, '
                f'closed_won:{fmt(r["closed_won"])}, '
                f'won_arr:{fmt(r["won_arr"])}, '
                f'dq_closed_lost:{fmt(r["dq_closed_lost"])}, '
                f'total_spend:{fmt(r["total_spend"])}}}'
            )
        else:
            stage = STAGE_MAP.get(fund, "Portco Recruiting")
            js_rows.append(
                f'  {{fund:"{fund}", stage:"{stage}", '
                f'total_portcos:0, enterprise_viable:0, enterprise_won:0, '
                f'open_pipe_count:0, open_pipe_amount:0, closed_won:0, '
                f'won_arr:0, dq_closed_lost:0, total_spend:0}}'
            )

    new_block = "const RAW_DATA = [\n" + ",\n".join(js_rows) + ",\n];"

    html = open("index.html").read()
    html = re.sub(r"const RAW_DATA = \[.*?\];", new_block, html, flags=re.DOTALL)
    open("index.html", "w").write(html)
    print(f"Updated {len(js_rows)} funds.")

if __name__ == "__main__":
    main()
