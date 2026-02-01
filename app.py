import streamlit as st
import pandas as pd
import requests
from datetime import datetime
from google.cloud import bigquery
from google.oauth2 import service_account
import json

# ======================
# CONFIG
# ======================
BASE_URL = "https://my411419-api.s4hana.cloud.sap"
SERVICE_PATH = "/sap/opu/odata/sap/API_SALES_ORDER_WITHOUT_CHARGE_SRV"
POST_ENDPOINT = SERVICE_PATH + "/A_SalesOrderWithoutCharge"

USERNAME = st.secrets["SAP_USERNAME"]
PASSWORD = st.secrets["SAP_PASSWORD"]

BQ_PROJECT = st.secrets["BQ_PROJECT"]
BQ_DATASET = st.secrets["BQ_DATASET"]
BQ_TABLE = "sap_foc_sales_orders"

# ======================
# BIGQUERY CLIENT
# ======================
credentials = service_account.Credentials.from_service_account_info(
    st.secrets["gcp_service_account"]
)

bq_client = bigquery.Client(
    project=BQ_PROJECT,
    credentials=credentials,
    location="asia-south1"
)

# ======================
# SESSION
# ======================
session = requests.Session()
session.auth = (USERNAME, PASSWORD)

# ======================
# HELPERS
# ======================
def fetch_csrf_token():
    headers = {"x-csrf-token": "Fetch", "Accept": "application/json"}
    r = session.get(BASE_URL + SERVICE_PATH, headers=headers)
    r.raise_for_status()
    return r.headers.get("x-csrf-token")

def sap_today_date():
    today = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
    return f"/Date({int(today.timestamp() * 1000)})/"

# 🔥 BUILD ONE PAYLOAD PER SoldToParty + PO
def build_group_payload(group_df, today_date):

    first = group_df.iloc[0]
    sold_to = str(first["SoldToParty"])
    po = str(first["PO_Number"])

    items = []

    for _, row in group_df.iterrows():
        items.append({
            "SalesOrderWithoutChargeItem": str(row["Item"]),
            "SlsOrdWthoutChrgItemCategory": "CBXN",
            "PurchaseOrderByCustomer": po,
            "Material": str(row["Material"]),
            "RequestedQuantity": str(row["Qty"]),
            "RequestedQuantityUnit": "EA",
            "TransactionCurrency": "INR",
            "NetAmount": "0",
            "Plant": str(row["Plant"]),
            "StorageLocation": str(row["StorageLocation"]),
            "ShippingPoint": str(row["ShippingPoint"])
        })

    return {
        "SalesOrderWithoutChargeType": "CBFD",
        "SalesOrganization": "2000",
        "DistributionChannel": "10",
        "OrganizationDivision": "00",
        "SoldToParty": sold_to,
        "PurchaseOrderByCustomer": po,
        "SalesOrderWithoutChargeDate": today_date,
        "RequestedDeliveryDate": today_date,
        "TransactionCurrency": "INR",
        "SDDocumentReason": "001",
        "ShippingCondition": "CC",
        "IncotermsClassification": "FOB",
        "IncotermsTransferLocation": "KA",
        "IncotermsLocation1": "KA",
        "to_Item": {
            "results": items
        }
    }

def save_to_bigquery(d):

    row = {
        "SalesOrderWithoutCharge": d.get("SalesOrderWithoutCharge"),
        "SalesOrderWithoutChargeType": d.get("SalesOrderWithoutChargeType"),
        "SalesOrganization": d.get("SalesOrganization"),
        "DistributionChannel": d.get("DistributionChannel"),
        "OrganizationDivision": d.get("OrganizationDivision"),
        "SoldToParty": d.get("SoldToParty"),
        "PurchaseOrderByCustomer": d.get("PurchaseOrderByCustomer"),
        "SalesOrderWithoutChargeDate": d.get("SalesOrderWithoutChargeDate"),
        "RequestedDeliveryDate": d.get("RequestedDeliveryDate"),
        "TransactionCurrency": d.get("TransactionCurrency"),
        "OverallSDProcessStatus": d.get("OverallSDProcessStatus"),
        "OverallTotalDeliveryStatus": d.get("OverallTotalDeliveryStatus"),
        "raw_response": json.dumps(d),
        "created_at": datetime.utcnow().isoformat()
    }

    table_id = f"{BQ_PROJECT}.{BQ_DATASET}.{BQ_TABLE}"

    job = bq_client.load_table_from_json(
        [row],
        table_id,
        location="asia-south1",
        job_config=bigquery.LoadJobConfig(
            write_disposition="WRITE_APPEND"
        )
    )
    job.result()

# ======================
# STREAMLIT UI
# ======================
st.title("🚀 SAP FOC Sales Order → BigQuery")

uploaded_file = st.file_uploader("Upload Excel", type=["xlsx"])

if uploaded_file:

    df = pd.read_excel(uploaded_file)
    grouped = df.groupby(["SoldToParty", "PO_Number"])

    preview = grouped.size().reset_index(name="ItemCount")
    st.subheader("📋 Preview (Grouped Orders)")
    st.dataframe(preview)

    if st.button("✅ Confirm & Submit"):

    csrf_token = fetch_csrf_token()
    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json",
        "x-csrf-token": csrf_token
    }

    today_date = sap_today_date()
    results = []

    # 🔥 GROUP BY SoldToParty + PO
    grouped = df.groupby(["SoldToParty", "PO_Number"])

    for (sold_to, po), group_df in grouped:

        payload = build_group_payload(group_df, today_date)

        response = session.post(
            BASE_URL + POST_ENDPOINT,
            json=payload,
            headers=headers
        )

        if response.status_code == 201:
            sap_d = response.json()["d"]

            save_to_bigquery(sap_d)

            results.append({
                "SoldToParty": sold_to,
                "SalesOrderWithoutCharge": sap_d["SalesOrderWithoutCharge"],
                "Status": "SUCCESS"
            })

            st.success(
                f"SUCCESS → SoldToParty {sold_to} | "
                f"Order {sap_d['SalesOrderWithoutCharge']} "
                f"({len(group_df)} items)"
            )

        else:
            results.append({
                "SoldToParty": sold_to,
                "SalesOrderWithoutCharge": None,
                "Status": "FAILED",
                "Error": response.text
            })

            st.error(
                f"FAILED → SoldToParty {sold_to} | Error: {response.text}"
            )

    st.success("Processing completed")
    st.dataframe(pd.DataFrame(results))
