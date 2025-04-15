import pandas as pd
from sqlalchemy import create_engine, inspect
from urllib.parse import quote_plus
import logging
import configparser

# Load configuration
config = configparser.ConfigParser()
config.read("config.ini")

# Configure logging
logging.basicConfig(
    filename=config["logging"]["log_file"],
    level=getattr(logging, config["logging"]["log_level"]),
    format="%(asctime)s - %(levelname)s - %(message)s",
)

# MySQL connection details
MYSQL_HOST = config["database"]["host"]
MYSQL_PORT = int(config["database"]["port"])
MYSQL_USER = config["database"]["user"]
MYSQL_PASSWORD = config["database"]["password"]
STAGING_SCHEMA = config["database"]["staging_db"]
TRANSFORM_SCHEMA = config["database"]["transform_db"]

# URL-encode password
encoded_password = quote_plus(MYSQL_PASSWORD)

# Create MySQL connection
engine = create_engine(
    f"mysql+pymysql://{MYSQL_USER}:{encoded_password}@{MYSQL_HOST}:{MYSQL_PORT}"
)

# Update engines for staging and transform schemas
staging_engine = create_engine(
    f"mysql+pymysql://{MYSQL_USER}:{encoded_password}@{MYSQL_HOST}:{MYSQL_PORT}/{STAGING_SCHEMA}"
)
transform_engine = create_engine(
    f"mysql+pymysql://{MYSQL_USER}:{encoded_password}@{MYSQL_HOST}:{MYSQL_PORT}/{TRANSFORM_SCHEMA}"
)

# Ensure transform schema exists
def create_schema(schema_name):
    try:
        with engine.connect() as conn:
            conn.execute(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")
        logging.info(f"Schema '{schema_name}' checked/created successfully.")
    except Exception as e:
        logging.error(f"Error creating schema '{schema_name}': {e}")

create_schema(TRANSFORM_SCHEMA)

# Function to fetch data from MySQL
def fetch_data(table_name):
    try:
        query = f"SELECT * FROM {table_name}"
        df = pd.read_sql(query, staging_engine)
        logging.info(f"Fetched data from {table_name} successfully.")
        return df
    except Exception as e:
        logging.error(f"Error fetching data from {table_name}: {e}")
        return None

# Transform date columns
def transform_date_columns(df):
    try:
        for col in df.columns:
            if "_date" in col.lower():
                df[col] = pd.to_datetime(df[col], errors="coerce")
                logging.info(f"Converted {col} to datetime format.")
        return df
    except Exception as e:
        logging.error(f"Error transforming date columns: {e}")
        return None

def transform_payments_table(df):
    try:
        # Convert columns to appropriate types
        df["payment_sequential"] = df["payment_sequential"].astype(int)
        df["payment_installments"] = df["payment_installments"].astype(int)
        df["payment_value"] = df["payment_value"].astype(float)
        df["payment_type"] = df["payment_type"].str.replace("_", " ").str.title()
        df.rename(columns={"order_id": "payment_id"}, inplace=True)
        df["payment_id"] = df["payment_id"].astype(str)

        # Group by payment_id and aggregate payment-related information
        df = df.groupby("payment_id").agg({
            "payment_sequential": lambda x: ", ".join(map(str, sorted(x))),
            "payment_installments": lambda x: ", ".join(map(str, sorted(x))),
            "payment_type": lambda x: ", ".join(x.iloc[x.argsort()].tolist()),
            "payment_value": "sum"
        }).reset_index()

        logging.info("Payments table transformed successfully.")
        return df
    except Exception as e:
        logging.error(f"Error transforming Payments Table: {e}")
        return None

def transform_feedback_table(df):
    try:
        # Convert columns to appropriate data types
        df["feedback_id"] = df["feedback_id"].astype(str)
        df["feedback_score"] = df["feedback_score"].astype(int)
        
        # Add the FeedbackID transformation logic here
        df['feedback_id'] = df.groupby('feedback_id').cumcount().astype(str) + "_" + df['feedback_id']
        
        # Transform date columns
        df = transform_date_columns(df)
        
        logging.info("Feedback table transformed successfully.")
        return df
    except Exception as e:
        logging.error(f"Error transforming Feedback Table: {e}")
        return None

def transform_products_table(df):
    try:
        df["product_id"] = df["product_id"].astype(str)
        df["product_category"] = df["product_category"].str.replace('_', ' ').str.title().astype(str)
        logging.info("Products table transformed successfully.")
        return df
    except Exception as e:
        logging.error(f"Error transforming Products Table: {e}")
        return None

def transform_sellers_table(df):
    try:
        df["seller_id"] = df["seller_id"].astype(str)
        df["seller_zip_code"] = df["seller_zip_code"].astype(str)
        df["seller_city"] = df["seller_city"].astype(str).str.title()
        df["seller_state"] = df["seller_state"].astype(str).str.title()
        logging.info("Sellers table transformed successfully.")
        return df
    except Exception as e:
        logging.error(f"Error transforming Sellers Table: {e}")
        return None

def transform_users_table(df):
    try:
        # Rename columns to match the desired schema
        df.rename(
            columns={
                "user_name": "user_id",
                "customer_zip_code": "user_zip_code",
                "customer_city": "user_city",
                "customer_state": "user_state"
            },
            inplace=True
        )
        
        # Convert columns to appropriate data types
        df["user_id"] = df["user_id"].astype(str)  # Convert user_id to string
        df["user_zip_code"] = df["user_zip_code"].astype(str)  # Convert zip code to string
        df["user_city"] = df["user_city"].astype(str).str.title()  # Capitalize city names
        df["user_state"] = df["user_state"].astype(str).str.title()  # Capitalize state names

        # Group by 'user_id' and aggregate user-related information
        df = df.groupby("user_id").agg({
            "user_zip_code": lambda x: ', '.join(x.unique()),  # Combine unique ZIP codes
            "user_city": lambda x: ', '.join(x.unique()),      # Combine unique city names
            "user_state": lambda x: ', '.join(x.unique())      # Combine unique state names
        }).reset_index()

        logging.info("Users table transformed successfully.")
        return df
    except Exception as e:
        logging.error(f"Error transforming Users Table: {e}")
        return None
    
def transform_orders_table(df, df_feedbacks):
    """
    Transform the orders table by merging it with the feedbacks table on order_id.
    """
    try:
        # Rename the 'user_name' column to 'user_id' for consistency
        df.rename(columns={"user_name": "user_id"}, inplace=True)
        
        # Convert key columns to string type
        for col in ['order_id', 'user_id', 'order_status']:
            df[col] = df[col].astype(str)
        
        # Transform date columns to ensure consistent datetime format
        df = transform_date_columns(df)
        
        # Merge the orders table with the feedbacks table on 'order_id'
        df = df.merge(
            df_feedbacks[['order_id', 'feedback_id']],  
            left_on='order_id',                        
            right_on='order_id',                       
            how='left'                                 
        )
        
        logging.info("Orders table transformed successfully.")
        return df
    except Exception as e:
        logging.error(f"Error transforming Orders Table: {e}")
        return None
    
def transform_order_item_table(df):
    try:
        # Convert columns to appropriate types
        df["price"] = df["price"].astype(float)
        df["shipping_cost"] = df["shipping_cost"].astype(float)

        # Drop the 'pickup_limit_date' column if it exists
        if "pickup_limit_date" in df.columns:
            df.drop(columns=["pickup_limit_date"], inplace=True)

        # Create a composite key for grouping: ProductID-SellerID
        df["ProductSellerPair"] = df["product_id"].astype(str) + "-" + df["seller_id"].astype(str)

        # Group by OrderID and ProductSellerPair, aggregating relevant columns
        df = df.groupby(["order_id", "ProductSellerPair"]).agg({
            "order_item_id": lambda x: ", ".join(map(str, sorted(x))),  
            "price": "sum",  
            "shipping_cost": "sum"  
        }).reset_index()

        # Split the ProductSellerPair back into ProductID and SellerID
        df[["product_id", "seller_id"]] = df["ProductSellerPair"].str.split("-", expand=True)

        # Drop the temporary ProductSellerPair column
        df.drop(columns=["ProductSellerPair"], inplace=True)

        # Calculate the Quantity column as the number of items in the aggregated order_item_id list
        df["quantity"] = df["order_item_id"].apply(lambda x: len(x.split(", ")))

        logging.info("Order Item table transformed successfully.")
        return df
    except Exception as e:
        logging.error(f"Error transforming Order Item Table: {e}")
        return None


# Load transformed data into MySQL
def load_data(df, table_name):
    try:
        df.to_sql(table_name, con=transform_engine, if_exists="replace", index=False)
        logging.info(f"Data loaded into {TRANSFORM_SCHEMA}.{table_name} successfully.")
    except Exception as e:
        logging.error(f"Error loading data into {TRANSFORM_SCHEMA}.{table_name}: {e}")


def create_date_and_time_tables():
    try:
        # Define date range
        start_date = pd.to_datetime("2016-04-09")
        end_date = pd.to_datetime("2018-10-17")
        
        # Create date dimension table
        date_range = pd.date_range(start=start_date, end=end_date, freq='D')
        date_df = pd.DataFrame(date_range, columns=["Date"])
        date_df["DateKey"] = date_df["Date"].apply(lambda x: int(x.strftime('%Y%m%d')))
        date_df["Day"] = date_df["Date"].apply(lambda x: x.day)
        date_df["DayName"] = date_df["Date"].apply(lambda x: x.strftime('%A'))
        date_df["Month"] = date_df["Date"].apply(lambda x: x.month)
        date_df["MonthName"] = date_df["Date"].apply(lambda x: x.strftime('%B'))
        date_df["Quarter"] = date_df["Date"].apply(lambda x: (x.month - 1) // 3 + 1)
        date_df["Year"] = date_df["Date"].apply(lambda x: x.year)
        date_df["DayOfWeek"] = date_df["Date"].apply(lambda x: x.weekday())
        date_df["IsWeekend"] = date_df["DayOfWeek"].apply(lambda x: 1 if x in [5, 6] else 0)
        date_df["Season"] = date_df["Month"].apply(
            lambda month: "Winter" if month in [12, 1, 2] else
                          "Spring" if month in [3, 4, 5] else
                          "Summer" if month in [6, 7, 8] else
                          "Fall"
        )

        # Store data in date_d table
        date_df.to_sql("date_d", transform_engine, if_exists="replace", index=False)
        logging.info("date_d table successfully created.")

        # Create time dimension table
        start_time = pd.to_datetime("00:00:00").time()
        end_time = pd.to_datetime("23:59:59").time()
        time_range = pd.date_range(start="00:00:00", end="23:59:59", freq='T').time
        times_df = pd.DataFrame(time_range, columns=["Time"])
        times_df["TimeKey"] = times_df["Time"].astype(str).str.replace(":", "").str.replace(".", "").astype(int)
        times_df["Hour"] = times_df["Time"].apply(lambda x: x.hour)
        times_df["Minute"] = times_df["Time"].apply(lambda x: x.minute)
        times_df["Second"] = times_df["Time"].apply(lambda x: x.second)
        times_df["AM_PM"] = times_df["Hour"].apply(lambda x: "AM" if x < 12 else "PM")
        times_df["TimeOfDay"] = pd.cut(times_df["Hour"], bins=[0, 6, 12, 18, 24], 
        labels=['Night', 'Morning', 'Afternoon', 'Evening'], right=False)

        # Store data in times_dd table
        times_df.to_sql("times_d", transform_engine, if_exists="replace", index=False)
        logging.info("times_d table successfully created.")

    except Exception as e:
        logging.error(f"Error creating date and time tables: {e}")

def add_primary_keys(engine):
    try:
        with engine.connect() as conn:
            conn.execute("ALTER TABLE date_d ADD PRIMARY KEY (DateKey);")
            conn.execute("ALTER TABLE times_d ADD PRIMARY KEY (TimeKey);")

        logging.info("Primary keys added successfully.")
    except Exception as e:
        logging.error(f"Error adding primary keys: {e}")
def main():
    # Define datasets and their configurations
    datasets = {
        "payment_dataset": {"transform": transform_payments_table, "target_table": "payment_d"},
        "feedback_dataset": {"transform": transform_feedback_table, "target_table": "feedback_d"},
        "products_dataset": {"transform": transform_products_table, "target_table": "products_d"},
        "seller_dataset": {"transform": transform_sellers_table, "target_table": "sellers_d"},
        "user_dataset": {"transform": transform_users_table, "target_table": "users_d"},
        "order_item_dataset": {"transform": transform_order_item_table, "target_table": "order_item_d"},
        "order_dataset": {"transform": transform_orders_table, "target_table": "orders_d", "depends_on": "feedback_d"}
    }

    # Fetch and transform feedback_dataset first (since it's a dependency)
    feedback_df = fetch_data("feedback_dataset")
    if feedback_df is None:
        logging.error("Feedback dataset could not be fetched. Exiting transformation process.")
        return
    
    transformed_feedback_df = transform_feedback_table(feedback_df)
    if transformed_feedback_df is None:
        logging.error("Feedback dataset transformation failed. Exiting transformation process.")
        return

    # Load the transformed feedback dataset
    load_data(transformed_feedback_df, "feedback_d")

    # Process all datasets
    for table_name, config in datasets.items():
        df = fetch_data(table_name)
        if df is None:
            logging.error(f"{table_name} could not be fetched. Skipping this dataset.")
            continue
        
        try:
            # Handle datasets with dependencies
            if "depends_on" in config:
                dependency = config["depends_on"]
                if dependency == "feedback_d":
                    # Pass the transformed feedback dataset as an additional argument
                    transformed_df = config["transform"](df, transformed_feedback_df)
                else:
                    logging.error(f"Unknown dependency '{dependency}' for {table_name}. Skipping this dataset.")
                    continue
            else:
                # For datasets without dependencies, apply the transformation directly
                transformed_df = config["transform"](df)
            
            # Load the transformed data into the target table
            if transformed_df is not None:
                load_data(transformed_df, config["target_table"])
                logging.info(f"{table_name} transformed and loaded successfully.")
            else:
                logging.error(f"Transformation returned None for {table_name}. Skipping this dataset.")
        except Exception as e:
            logging.error(f"Error processing {table_name}: {e}")
            continue

    # Create date and time dimension tables
    create_date_and_time_tables()
    logging.info("Date and time dimension tables created successfully.")

    # Add primary keys to all tables
    add_primary_keys(transform_engine)
    logging.info("Primary keys added successfully.")
    logging.info("All transformations completed successfully.")


if __name__ == "__main__":
    main()
    print("✅ All transformations completed successfully!")
