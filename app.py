import streamlit as st
import pandas as pd
import requests
from datetime import datetime
from google.cloud import bigquery
from google.oauth2 import service_account
import json
from datetime import datetime, timedelta, timezone

# ======================
# CONFIG
# ======================
BASE_URL = "https://my411419-api.s4hana.cloud.sap"
SERVICE_PATH = "/sap/opu/odata/sap/API_SALES_ORDER_WITHOUT_CHARGE_SRV"
POST_ENDPOINT = SERVICE_PATH + "/A_SalesOrderWithoutCharge"

BQ_TABLE = "sap_foc_sales_orders"

# ======================
# SAFE LAZY CLIENTS (🔥 FIX)
# ======================
@st.cache_resource
def get_bq_client():
    credentials = service_account.Credentials.from_service_account_info(
        st.secrets["gcp_service_account"]
    )

    return bigquery.Client(
        project=BQ_PROJECT,
        credentials=credentials,
        location="asia-south1"
    )



@st.cache_resource
def get_sap_session():
    s = requests.Session()
    s.auth = (
        st.secrets["SAP_USERNAME"],
        st.secrets["SAP_PASSWORD"]
    )
    return s


# ======================
# HELPERS
# ======================
def fetch_csrf_token():
    session = get_sap_session()
    headers = {"x-csrf-token": "Fetch", "Accept": "application/json"}
    r = session.get(BASE_URL + SERVICE_PATH, headers=headers)
    r.raise_for_status()
    return r.headers.get("x-csrf-token")


def sap_today_date():
    ist = timezone(timedelta(hours=5, minutes=30))
    today_ist = datetime.now(ist).replace(
        hour=0, minute=0, second=0, microsecond=0
    )
    return f"/Date({int(today_ist.timestamp() * 1000)})/"


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
        "OrganizationDivision": "50",
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
    bq_client = get_bq_client()

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

    table_id = f"{st.secrets['BQ_PROJECT']}.{st.secrets['BQ_DATASET']}.{BQ_TABLE}"

    job = bq_client.load_table_from_json(
        [row],
        table_id,
        job_config=bigquery.LoadJobConfig(
            write_disposition="WRITE_APPEND"
        )
    )
    job.result()


def save_items_to_bigquery(order_no, group_df):
    bq_client = get_bq_client()

    rows = []

    for _, row in group_df.iterrows():
        rows.append({
            "SalesOrderWithoutCharge": order_no,
            "ItemNumber": str(row["Item"]),
            "Material": str(row["Material"]),
            "RequestedQuantity": str(row["Qty"]),
            "Plant": str(row["Plant"]),
            "StorageLocation": str(row["StorageLocation"]),
            "ShippingPoint": str(row["ShippingPoint"]),
            "created_at": datetime.utcnow().isoformat()
        })

    table_id = (
        f"{st.secrets['BQ_PROJECT']}."
        f"{st.secrets['BQ_DATASET']}."
        f"sap_foc_sales_order_items"
    )

    job = bq_client.load_table_from_json(
        rows,
        table_id,
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

    # 🔍 GROUPED PREVIEW
    st.subheader("📋 Preview – Grouped by SoldToParty")

    grouped_preview = (
        df.groupby(["SoldToParty", "PO_Number"])
          .agg(
              ItemCount=("Material", "count"),
              Materials=("Material", lambda x: ", ".join(map(str, x)))
          )
          .reset_index()
    )

    st.dataframe(grouped_preview)

    st.info(
        "ℹ️ Each row above will create **ONE SAP Sales Order** "
        "with the listed materials as items."
    )

    confirm = st.checkbox("I have reviewed the grouped data and want to proceed")

if confirm and st.button("🚀 Submit to SAP"):

    session = get_sap_session()
    csrf_token = fetch_csrf_token()

    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json",
        "x-csrf-token": csrf_token
    }

    today_date = sap_today_date()
    results = []

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
            save_items_to_bigquery(
                sap_d["SalesOrderWithoutCharge"],
                group_df
            )

            results.append({
                "SoldToParty": sold_to,
                "PO_Number": po,
                "SalesOrderWithoutCharge": sap_d["SalesOrderWithoutCharge"],
                "Items": len(group_df),
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
                "PO_Number": po,
                "SalesOrderWithoutCharge": None,
                "Items": len(group_df),
                "Status": "FAILED"
            })

            st.error(
                f"FAILED → SoldToParty {sold_to} | {response.text}"
            )

    st.subheader("📊 Processing Summary")
    st.dataframe(pd.DataFrame(results))
