import pandas as pd
from sqlalchemy import create_engine, event, text 
import os
from sqlalchemy.engine import URL

# 1. Configuration: Connection Details
DB_CONFIG = {
    "driver": "ODBC Driver 18 for SQL Server",
    "server": "127.0.0.1", 
    "port": "1433",
    "database": "RetailWarehouse",
    "username": "sa",
    "password": "TpBirdie1801@"
}

connection_string = URL.create(
    "mssql+pyodbc",
    username=DB_CONFIG["username"],
    password=DB_CONFIG["password"],
    host=DB_CONFIG["server"],
    port=DB_CONFIG["port"],
    database=DB_CONFIG["database"],
    query={
        "driver": DB_CONFIG["driver"],
        "Encrypt": "no",
        "TrustServerCertificate": "yes",
    },
)
engine = create_engine(connection_string)


@event.listens_for(engine, "before_cursor_execute")
def receive_before_cursor_execute(conn, cursor, statement, parameters, context, executemany):
    if executemany:
        cursor.fast_executemany = True

# 2. Mapping Configuration: CSV File -> SQL Table -> Column Mapping
tables_to_load = {
    "olist_customers.csv": {
        "table_name": "olist_cust",
        "mapping": {
            "customer_id": "cst_cust_id",
            "customer_unique_id": "cst_cust_unique_id",
            "customer_zip_code_prefix": "cst_zip_code_prefix",
            "customer_city": "cst_city",
            "customer_state": "cst_state"
        }
    },
    "olist_geolocation.csv": {
        "table_name": "olist_geo",
        "mapping": {
            "geolocation_zip_code_prefix": "geo_zip_code_prefix",
            "geolocation_lat": "geo_lat",
            "geolocation_lng": "geo_lng",
            "geolocation_city": "geo_city",
            "geolocation_state": "geo_state"
        }
    },
    "olist_order_items.csv": {
        "table_name": "olist_ord_item",
        "mapping": {
            "order_id": "oi_ord_id",
            "order_item_id": "oi_ord_item_id",
            "product_id": "oi_prd_id",
            "seller_id": "oi_seller_id",
            "shipping_limit_date": "oi_ship_limit_dt",
            "price": "oi_price",
            "freight_value": "oi_freight_val"
        }
    },
    "olist_order_payments.csv": {
        "table_name": "olist_ord_pay",
        "mapping": {
            "order_id": "op_ord_id",
            "payment_sequential": "op_pay_seq",
            "payment_type": "op_pay_type",
            "payment_installments": "op_pay_inst",
            "payment_value" : "op_pay_val"
        }
    },
    "olist_order_reviews.csv": {
        "table_name": "olist_ord_rev",
        "mapping": {
            "review_id": "or_rev_id",
            "order_id": "or_ord_id",
            "review_score": "or_rev_score",
            "review_comment_title": "or_rev_cmt_titile",
            "review_comment_message": "or_rev_cmt_msg",
            "review_creation_date": "or_rev_create_dt",
            "review_answer_timestamp": "or_rev_ans_ts"
        }
    },
    "olist_orders.csv": {
        "table_name": "olist_ord",
        "mapping": {
            "order_id": "ord_ord_id",
            "customer_id": "ord_cust_id",
            "order_status": "ord_status",
            "order_purchase_timestamp": "ord_purchase_ts",
            "order_approved_at": "ord_approved_ts",
            "order_delivered_carrier_date": "ord_del_carrier_dt",
            "order_delivered_customer_date": "ord_del_cust_dt",
            "order_estimated_delivery_date": "ord_est_del_dt"
        }
    },
    "olist_products.csv": {
        "table_name": "olist_prd",
        "mapping": {
            "product_id": "prd_prd_id",
            "product_category_name": "prd_cat_name",
            "product_name_lenght": "prd_name_len",
            "product_description_lenght": "prd_desc_len",
            "product_photos_qty": "prd_photos_qty",
            "product_weight_g": "prd_weight_g",
            "product_length_cm": "prd_len_cm",
            "product_height_cm": "prd_height_cm",
            "product_width_cm" : "prd_width_cm"
        }
    },
    "olist_sellers.csv": {
        "table_name": "olist_sel",
        "mapping": {
            "seller_id": "sel_sel_id",
            "seller_zip_code_prefix": "sel_zip_code_prefix",
            "seller_city": "sel_city",
            "seller_state": "sel_state"
        }
    },
    "olist_product_category_name_translation.csv": {
        "table_name": "olist_prd_cat_map",
        "mapping": {
            "product_category_name": "pcm_cat_name",
            "product_category_name_english": "pcm_cat_name_en"
        }
    }
}

def ingest_data(source_path):
    print("Starting Batch Ingestion to Bronze Layer...")
    
    for file_name, config in tables_to_load.items():
        file_path = os.path.join(source_path, file_name)
        
        if os.path.exists(file_path):
            print(f"Reading {file_name}...")
            # Read CSV
            df = pd.read_csv(file_path,dtype={'geolocation_lat': str, 'geolocation_lng': str})
            
            # Rename columns based on our mapping
            df = df.rename(columns=config["mapping"])
            
            # Keep only the columns we defined in our mapping
            df = df[list(config["mapping"].values())]
            
            # Truncate and Load (Full Load Strategy)
            print(f"Loading into bronze.{config['table_name']}...")
            with engine.begin() as conn:
                conn.execute(text(f"TRUNCATE TABLE bronze.{config['table_name']}"))
                df.to_sql(config['table_name'], schema='bronze', con=conn, if_exists='append', index=False)
            
            print(f"{file_name} loaded successfully.")
        else:
            print(f"Warning: {file_name} not found in {source_path}")

    print("All files processed.")


if __name__ == "__main__":
    CSV_FOLDER_PATH = "./datasets/olist/" 
    ingest_data(CSV_FOLDER_PATH)