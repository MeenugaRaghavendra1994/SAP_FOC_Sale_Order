import streamlit as st
import pandas as pd
import requests
import json
from datetime import datetime, timedelta, timezone
from google.cloud import bigquery
from google.oauth2 import service_account

# =====================================================
# CONFIG
# =====================================================
BASE_URL = "https://my411419-api.s4hana.cloud.sap"
SERVICE_PATH = "/sap/opu/odata/sap/API_SALES_ORDER_WITHOUT_CHARGE_SRV"
POST_ENDPOINT = SERVICE_PATH + "/A_SalesOrderWithoutCharge"

HEADER_TABLE = "sap_foc_sales_orders"
ITEM_TABLE = "sap_foc_sales_order_items"

# =====================================================
# LAZY CLIENTS (NO STARTUP HANG)
# =====================================================
@st.cache_resource
def get_bq_client():
    credentials = service_account.Credentials.from_service_account_info(
        st.secrets["gcp_service_account"]
    )
    return bigquery.Client(
        project=st.secrets["BQ_PROJECT"],
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


# =====================================================
# HELPERS
# =====================================================
def fetch_csrf_token():
    session = get_sap_session()
    headers = {"x-csrf-token": "Fetch", "Accept": "application/json"}
    r = session.get(BASE_URL + SERVICE_PATH, headers=headers)
    r.raise_for_status()
    return r.headers.get("x-csrf-token")


def sap_today_date():
    ist = timezone(timedelta(hours=5, minutes=30))
    today = datetime.now(ist).replace(
        hour=0, minute=0, second=0, microsecond=0
    )
    return f"/Date({int(today.timestamp() * 1000)})/"


def build_group_payload(group_df, today_date):
    first = group_df.iloc[0]
    sold_to = str(first["SoldToParty"])
    po = str(first["PO_Number"])

    items = []
    for _, r in group_df.iterrows():
        items.append({
            "SalesOrderWithoutChargeItem": str(r["Item"]),
            "SlsOrdWthoutChrgItemCategory": "CBXN",
            "PurchaseOrderByCustomer": po,
            "Material": str(r["Material"]),
            "RequestedQuantity": str(r["Qty"]),
            "RequestedQuantityUnit": "EA",
            "TransactionCurrency": "INR",
            "NetAmount": "0",
            "Plant": str(r["Plant"]),
            "StorageLocation": str(r["StorageLocation"]),
            "ShippingPoint": str(r["ShippingPoint"])
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
        "to_Item": {"results": items}
    }


def save_header_to_bigquery(d):
    bq = get_bq_client()

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
        "raw_response": json.dumps(d),
        "created_at": datetime.utcnow().isoformat()
    }

    table_id = (
        f"{st.secrets['BQ_PROJECT']}."
        f"{st.secrets['BQ_DATASET']}."
        f"{HEADER_TABLE}"
    )

    job = bq.load_table_from_json(
        [row],
        table_id,
        job_config=bigquery.LoadJobConfig(
            write_disposition="WRITE_APPEND"
        )
    )
    job.result()


def save_items_to_bigquery(order_no, group_df):
    bq = get_bq_client()

    rows = []
    for _, r in group_df.iterrows():
        rows.append({
            "SalesOrderWithoutCharge": order_no,
            "ItemNumber": str(r["Item"]),
            "Material": str(r["Material"]),
            "RequestedQuantity": str(r["Qty"]),
            "Plant": str(r["Plant"]),
            "StorageLocation": str(r["StorageLocation"]),
            "ShippingPoint": str(r["ShippingPoint"]),
            "created_at": datetime.utcnow().isoformat()
        })

    table_id = (
        f"{st.secrets['BQ_PROJECT']}."
        f"{st.secrets['BQ_DATASET']}."
        f"{ITEM_TABLE}"
    )

    job = bq.load_table_from_json(
        rows,
        table_id,
        job_config=bigquery.LoadJobConfig(
            write_disposition="WRITE_APPEND"
        )
    )
    job.result()


# =====================================================
# STREAMLIT UI
# =====================================================
st.title("🚀 SAP FOC Sales Order → BigQuery")

uploaded_file = st.file_uploader("Upload Excel", type=["xlsx"])

if uploaded_file is not None:

    df = pd.read_excel(uploaded_file)

    st.subheader("📋 Preview – Grouped by SoldToParty")
    preview = (
        df.groupby(["SoldToParty", "PO_Number"])
          .agg(
              ItemCount=("Material", "count"),
              Materials=("Material", lambda x: ", ".join(map(str, x)))
          )
          .reset_index()
    )
    st.dataframe(preview)

    confirm = st.checkbox("I have reviewed the grouped data and want to proceed")

    if confirm and st.button("🚀 Submit to SAP"):

        session = get_sap_session()
        csrf = fetch_csrf_token()

        headers = {
            "Content-Type": "application/json",
            "Accept": "application/json",
            "x-csrf-token": csrf
        }

        today = sap_today_date()
        results = []

        grouped = df.groupby(["SoldToParty", "PO_Number"])

        for (sold_to, po), gdf in grouped:

            payload = build_group_payload(gdf, today)

            resp = session.post(
                BASE_URL + POST_ENDPOINT,
                json=payload,
                headers=headers
            )

            if resp.status_code == 201:
                sap_d = resp.json()["d"]

                save_header_to_bigquery(sap_d)
                save_items_to_bigquery(
                    sap_d["SalesOrderWithoutCharge"],
                    gdf
                )

                results.append({
                    "SoldToParty": sold_to,
                    "PO_Number": po,
                    "SalesOrderWithoutCharge": sap_d["SalesOrderWithoutCharge"],
                    "Items": len(gdf),
                    "Status": "SUCCESS"
                })

                st.success(
                    f"SUCCESS → {sold_to} | "
                    f"Order {sap_d['SalesOrderWithoutCharge']} "
                    f"({len(gdf)} items)"
                )

            else:
                results.append({
                    "SoldToParty": sold_to,
                    "PO_Number": po,
                    "SalesOrderWithoutCharge": None,
                    "Items": len(gdf),
                    "Status": "FAILED"
                })

                st.error(
                    f"FAILED → {sold_to} | {resp.text}"
                )

        st.subheader("📊 Processing Summary")
        st.dataframe(pd.DataFrame(results))
