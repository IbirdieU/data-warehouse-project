import pandas as pd
from sqlalchemy import create_engine, event, text 
import os
from urllib.parse import quote_plus
from sqlalchemy.engine import URL
from dotenv import load_dotenv
import sqlalchemy as sa
import time,datetime



load_dotenv()


# 1. Configuration
DB_SERVER = os.getenv('DB_SERVER')
DB_NAME = os.getenv('DB_NAME')
DB_USER = os.getenv('DB_USER')
DB_PASS = os.getenv('DB_PASS')
DB_PORT = os.getenv('DB_PORT')

safe_password = quote_plus(DB_PASS)

connection_url = (
    f"mssql+pyodbc://{DB_USER}:{safe_password}@{DB_SERVER}:{DB_PORT}/{DB_NAME}"
    "?driver=ODBC+Driver+17+for+SQL+Server"
)

engine = sa.create_engine(connection_url, fast_executemany=True)

@event.listens_for(engine, "before_cursor_execute")
def receive_before_cursor_execute(conn, cursor, statement, parameters, context, executemany):
    if executemany:
        cursor.fast_executemany = True

# 2. Mapping Configuration: CSV File -> SQL Table -> Column Mapping
tables_to_load = {
    "olist_customers_dataset.csv": {
        "table_name": "olist_cust",
        "mapping": {
            "customer_id": "cst_cust_id",
            "customer_unique_id": "cst_cust_unique_id",
            "customer_zip_code_prefix": "cst_zip_code_prefix",
            "customer_city": "cst_city",
            "customer_state": "cst_state"
        }
    },
    "olist_geolocation_dataset.csv": {
        "table_name": "olist_geo",
        "mapping": {
            "geolocation_zip_code_prefix": "geo_zip_code_prefix",
            "geolocation_lat": "geo_lat",
            "geolocation_lng": "geo_lng",
            "geolocation_city": "geo_city",
            "geolocation_state": "geo_state"
        }
    },
    "olist_order_items_dataset.csv": {
        "table_name": "olist_ord_item",
        "mapping": {
            "order_id": "oi_ord_id",
            "order_item_id": "oi_ord_item_id",
            "product_id": "oi_prd_id",
            "seller_id": "oi_sel_id",
            "shipping_limit_date": "oi_ship_limit_dt",
            "price": "oi_price",
            "freight_value": "oi_freight_val"
        }
    },
    "olist_order_payments_dataset.csv": {
        "table_name": "olist_ord_pay",
        "mapping": {
            "order_id": "op_ord_id",
            "payment_sequential": "op_pay_seq",
            "payment_type": "op_pay_type",
            "payment_installments": "op_pay_inst",
            "payment_value" : "op_pay_val"
        }
    },
    "olist_order_reviews_dataset.csv": {
        "table_name": "olist_ord_rev",
        "mapping": {
            "review_id": "or_rev_id",
            "order_id": "or_ord_id",
            "review_score": "or_rev_score",
            "review_comment_title": "or_rev_cmt_title",
            "review_comment_message": "or_rev_cmt_msg",
            "review_creation_date": "or_rev_create_dt",
            "review_answer_timestamp": "or_rev_ans_ts"
        }
    },
    "olist_orders_dataset.csv": {
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
    "olist_products_dataset.csv": {
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
    "olist_sellers_dataset.csv": {
        "table_name": "olist_sel",
        "mapping": {
            "seller_id": "sel_sel_id",
            "seller_zip_code_prefix": "sel_zip_code_prefix",
            "seller_city": "sel_city",
            "seller_state": "sel_state"
        }
    },
    "product_category_name_translation.csv": {
        "table_name": "olist_prd_cat_map",
        "mapping": {
            "product_category_name": "pcm_cat_name",
            "product_category_name_english": "pcm_cat_name_en"
        }
    }
}

def ingest_data(source_path):
    overall_start = time.time()
    print("-" * 50)
    print("Starting Batch Ingestion to Bronze Layer...")
    
    for file_name, config in tables_to_load.items():
        file_path = os.path.join(source_path, file_name)
        
        if os.path.exists(file_path):
            table_start = time.time()
            print(f"Reading {file_name}...")
            # Read CSV
            df = pd.read_csv(file_path,dtype = str)
            
            # Rename columns based on our mapping
            df = df.rename(columns=config["mapping"])
            
            # Keep only the columns defined in our mapping
            df = df[list(config["mapping"].values())]
            
            # Truncate and Load
            print(f"Loading into bronze.{config['table_name']}...")
            try:
                with engine.begin() as conn:
                    conn.execute(text(f"TRUNCATE TABLE bronze.{config['table_name']}"))
                    df.to_sql(config['table_name'], schema='bronze', con=conn, if_exists='append', index=False)
                
                table_end = time.time()
                duration = table_end - table_start
                mins, secs = divmod(duration, 60)
                if mins > 0:
                    print(f"✅ {file_name} loaded in {int(mins)}m {secs:.2f}s.")
                else:
                    print(f"✅ {file_name} loaded in {secs:.2f} seconds.")
                
            except Exception as e:
                print(f"❌ Error loading {file_name}: {e}")
            
            print("-" * 40)
        else:
            print(f"Warning: {file_name} not found in {source_path}")

    overall_end = time.time()
    total_time = overall_end - overall_start
    formatted_total = str(datetime.timedelta(seconds=int(total_time)))
    print("-" * 60)
    print(f"✨ All files processed in: {formatted_total} (HH:MM:SS)")


if __name__ == "__main__":
    CSV_FOLDER_PATH = "./datasets/olist/" 
    ingest_data(CSV_FOLDER_PATH)