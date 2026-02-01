import streamlit as st
import pandas as pd
import requests
from datetime import datetime
from google.cloud import bigquery
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
bq_client = bigquery.Client(project=BQ_PROJECT)

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

def build_payload(row, today_date):
    sold_to = str(row["SoldToParty"])
    po = str(row["PO_Number"])

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
            "results": [{
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
            }]
        }
    }

def save_to_bigquery(d):
    row = {
        "SalesOrderWithoutCharge": d.get("SalesOrderWithoutCharge"),
        "SalesOrderWithoutChargeType": d.get("SalesOrderWithoutChargeType"),
        "SalesOrganization": d.get("SalesOrganization"),
        "DistributionChannel": d.get("DistributionChannel"),
        "OrganizationDivision": d.get("OrganizationDivision"),
        "SalesGroup": d.get("SalesGroup"),
        "SalesOffice": d.get("SalesOffice"),
        "SalesDistrict": d.get("SalesDistrict"),
        "SoldToParty": d.get("SoldToParty"),
        "CreationDate": d.get("CreationDate"),
        "CreatedByUser": d.get("CreatedByUser"),
        "LastChangeDate": d.get("LastChangeDate"),
        "LastChangeDateTime": d.get("LastChangeDateTime"),
        "PurchaseOrderByCustomer": d.get("PurchaseOrderByCustomer"),
        "CustomerPurchaseOrderType": d.get("CustomerPurchaseOrderType"),
        "CustomerPurchaseOrderDate": d.get("CustomerPurchaseOrderDate"),
        "SalesOrderWithoutChargeDate": d.get("SalesOrderWithoutChargeDate"),
        "TotalNetAmount": d.get("TotalNetAmount"),
        "TransactionCurrency": d.get("TransactionCurrency"),
        "SDDocumentReason": d.get("SDDocumentReason"),
        "RequestedDeliveryDate": d.get("RequestedDeliveryDate"),
        "DeliveryDateTypeRule": d.get("DeliveryDateTypeRule"),
        "ShippingCondition": d.get("ShippingCondition"),
        "CompleteDeliveryIsDefined": bool(d.get("CompleteDeliveryIsDefined")),
        "ShippingType": d.get("ShippingType"),
        "DeliveryBlockReason": d.get("DeliveryBlockReason"),
        "HeaderBillingBlockReason": d.get("HeaderBillingBlockReason"),
        "IncotermsClassification": d.get("IncotermsClassification"),
        "IncotermsTransferLocation": d.get("IncotermsTransferLocation"),
        "IncotermsLocation1": d.get("IncotermsLocation1"),
        "IncotermsLocation2": d.get("IncotermsLocation2"),
        "IncotermsVersion": d.get("IncotermsVersion"),
        "CostCenter": d.get("CostCenter"),
        "ReferenceSDDocument": d.get("ReferenceSDDocument"),
        "AccountingDocExternalReference": d.get("AccountingDocExternalReference"),
        "ReferenceSDDocumentCategory": d.get("ReferenceSDDocumentCategory"),
        "OverallSDProcessStatus": d.get("OverallSDProcessStatus"),
        "OverallTotalDeliveryStatus": d.get("OverallTotalDeliveryStatus"),
        "OverallSDDocumentRejectionSts": d.get("OverallSDDocumentRejectionSts"),

        # 🔥 FULL SAP RESPONSE (SAFE)
        "raw_response": d,

        # 🔥 AUDIT
        "created_at": datetime.utcnow()
    }

    table_id = f"{BQ_PROJECT}.{BQ_DATASET}.{BQ_TABLE}"
    errors = bq_client.insert_rows_json(table_id, [row])
    if errors:
        raise RuntimeError(errors)


# ======================
# STREAMLIT UI
# ======================
st.title("🚀 SAP FOC Sales Order → BigQuery")

uploaded_file = st.file_uploader("Upload Excel", type=["xlsx"])

if uploaded_file and st.button("Process"):
    df = pd.read_excel(uploaded_file)
    csrf_token = fetch_csrf_token()

    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json",
        "x-csrf-token": csrf_token
    }

    today_date = sap_today_date()
    results = []

    for _, row in df.iterrows():
        payload = build_payload(row, today_date)
        response = session.post(
            BASE_URL + POST_ENDPOINT,
            json=payload,
            headers=headers
        )
if response.status_code == 201:
    sap_d = response.json().get("d", {})

    bq_row = {
        "SalesOrderWithoutCharge": sap_d.get("SalesOrderWithoutCharge"),
        "SalesOrderWithoutChargeType": sap_d.get("SalesOrderWithoutChargeType"),
        "SalesOrganization": sap_d.get("SalesOrganization"),
        "DistributionChannel": sap_d.get("DistributionChannel"),
        "OrganizationDivision": sap_d.get("OrganizationDivision"),
        "SalesGroup": sap_d.get("SalesGroup"),
        "SalesOffice": sap_d.get("SalesOffice"),
        "SalesDistrict": sap_d.get("SalesDistrict"),
        "SoldToParty": sap_d.get("SoldToParty"),
        "CreationDate": sap_d.get("CreationDate"),
        "CreatedByUser": sap_d.get("CreatedByUser"),
        "LastChangeDate": sap_d.get("LastChangeDate"),
        "LastChangeDateTime": sap_d.get("LastChangeDateTime"),
        "PurchaseOrderByCustomer": sap_d.get("PurchaseOrderByCustomer"),
        "CustomerPurchaseOrderType": sap_d.get("CustomerPurchaseOrderType"),
        "CustomerPurchaseOrderDate": sap_d.get("CustomerPurchaseOrderDate"),
        "SalesOrderWithoutChargeDate": sap_d.get("SalesOrderWithoutChargeDate"),
        "TotalNetAmount": sap_d.get("TotalNetAmount"),
        "TransactionCurrency": sap_d.get("TransactionCurrency"),
        "SDDocumentReason": sap_d.get("SDDocumentReason"),
        "RequestedDeliveryDate": sap_d.get("RequestedDeliveryDate"),
        "DeliveryDateTypeRule": sap_d.get("DeliveryDateTypeRule"),
        "ShippingCondition": sap_d.get("ShippingCondition"),
        "CompleteDeliveryIsDefined": bool(sap_d.get("CompleteDeliveryIsDefined")),
        "ShippingType": sap_d.get("ShippingType"),
        "DeliveryBlockReason": sap_d.get("DeliveryBlockReason"),
        "HeaderBillingBlockReason": sap_d.get("HeaderBillingBlockReason"),
        "IncotermsClassification": sap_d.get("IncotermsClassification"),
        "IncotermsTransferLocation": sap_d.get("IncotermsTransferLocation"),
        "IncotermsLocation1": sap_d.get("IncotermsLocation1"),
        "IncotermsLocation2": sap_d.get("IncotermsLocation2"),
        "IncotermsVersion": sap_d.get("IncotermsVersion"),
        "CostCenter": sap_d.get("CostCenter"),
        "ReferenceSDDocument": sap_d.get("ReferenceSDDocument"),
        "AccountingDocExternalReference": sap_d.get("AccountingDocExternalReference"),
        "ReferenceSDDocumentCategory": sap_d.get("ReferenceSDDocumentCategory"),
        "OverallSDProcessStatus": sap_d.get("OverallSDProcessStatus"),
        "OverallTotalDeliveryStatus": sap_d.get("OverallTotalDeliveryStatus"),
        "OverallSDDocumentRejectionSts": sap_d.get("OverallSDDocumentRejectionSts"),

        # 🔥 FULL RAW RESPONSE (exact SAP JSON)
        "raw_response": sap_d,

        # 🔥 AUDIT COLUMN
        "created_at": datetime.utcnow()
    }

    save_to_bigquery(bq_row)

    results.append({
        "SoldToParty": sap_d.get("SoldToParty"),
        "SalesOrderWithoutCharge": sap_d.get("SalesOrderWithoutCharge"),
        "Status": "SUCCESS"
    })
else:
    results.append({
        "SoldToParty": row["SoldToParty"],
        "SalesOrderWithoutCharge": None,
        "Status": "FAILED",
        "Error": response.text
    })


    st.success("Processing completed")
    st.dataframe(pd.DataFrame(results))
    
