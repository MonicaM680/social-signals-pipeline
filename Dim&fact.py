import pandas as pd
from sqlalchemy import create_engine, text
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
TRANSFORM_SCHEMA = config["database"]["transform_db"]

# URL-encode password
encoded_password = quote_plus(MYSQL_PASSWORD)

# Create MySQL connection for transform schema
transform_engine = create_engine(
    f"mysql+pymysql://{MYSQL_USER}:{encoded_password}@{MYSQL_HOST}:{MYSQL_PORT}/{TRANSFORM_SCHEMA}"
)

# Function to create a user dimension table with a primary key
def create_dim_users(engine):
    """
    Creates the dim_users table with UserID as the primary key.
    """
    with engine.begin() as connection:  
        connection.execute(text("DROP TABLE IF EXISTS fact_order_items;"))
    try:
        with engine.begin() as connection:
            # Drop the existing dim_users table if it exists
            connection.execute(text("DROP TABLE IF EXISTS dim_users;"))
            
            # Create the dim_users table by selecting relevant columns from users_d
            connection.execute(text("""
                CREATE TABLE dim_users AS
                SELECT user_id AS UserID, user_zip_code AS UserZIPCode, user_city AS UserCity, user_state AS UserState
                FROM users_d;
            """))
            
            # Modify UserID to VARCHAR(255) and set it as the primary key
            connection.execute(text("ALTER TABLE dim_users MODIFY COLUMN UserID VARCHAR(255);"))
            connection.execute(text("ALTER TABLE dim_users ADD PRIMARY KEY (UserID);"))
        
        logging.info("Dimension Table 'dim_users' created successfully.")
    except Exception as e:
        logging.error(f"Error creating dim_users: {e}")
        raise

# Function to create a feedbacks dimension table with a primary key
def create_dim_feedbacks(engine):
    """
    Creates the dim_feedbacks table with FeedbackID as the primary key.
    """
    try:
        with engine.begin() as connection:
            # Drop the existing dim_feedbacks table if it exists
            connection.execute(text("DROP TABLE IF EXISTS dim_feedbacks;"))
            
            # Create the dim_feedbacks table by selecting relevant columns from feedback_d
            connection.execute(text("""
                CREATE TABLE dim_feedbacks AS
                SELECT feedback_id AS FeedbackID, feedback_score AS FeedbackScore,
                       feedback_form_sent_date AS FeedbackFormSentDate, feedback_answer_date AS FeedbackAnswerDate
                FROM feedback_d;
            """))
            
            # Modify FeedbackID to VARCHAR(255) and set it as the primary key
            connection.execute(text("ALTER TABLE dim_feedbacks MODIFY COLUMN FeedbackID VARCHAR(255);"))
            connection.execute(text("ALTER TABLE dim_feedbacks ADD PRIMARY KEY (FeedbackID);"))
        
        logging.info("Dimension Table 'dim_feedbacks' created successfully.")
    except Exception as e:
        logging.error(f"Error creating dim_feedbacks: {e}")
        raise



# Function to create a payments dimension table with a primary key
def create_dim_payments(engine):
    """
    Creates the dim_payments table with PaymentID as the primary key.
    """
    try:
        with engine.begin() as connection:
            # Drop the existing dim_payments table if it exists
            connection.execute(text("DROP TABLE IF EXISTS dim_payments;"))
            
            # Create the dim_payments table by selecting relevant columns from payment_d
            connection.execute(text("""
                CREATE TABLE dim_payments AS
                SELECT payment_id AS PaymentID, payment_value AS PaymentValue,
                       payment_installments AS PaymentInstallments, payment_sequential AS PaymentSequential,
                       payment_type AS PaymentType
                FROM payment_d;
            """))
            
            # Modify PaymentID to VARCHAR(255) and set it as the primary key
            connection.execute(text("ALTER TABLE dim_payments MODIFY COLUMN PaymentID VARCHAR(255);"))
            connection.execute(text("ALTER TABLE dim_payments ADD PRIMARY KEY (PaymentID);"))
        
        logging.info("Dimension Table 'dim_payments' created successfully.")
    except Exception as e:
        logging.error(f"Error creating dim_payments: {e}")
        raise

# Function to create a products dimension table with a primary key
def create_dim_products(engine):
    """
    Creates the dim_products table with ProductID as the primary key.
    """
    try:
        with engine.begin() as connection:
            # Drop the existing dim_products table if it exists
            connection.execute(text("DROP TABLE IF EXISTS dim_products;"))
            
            # Create the dim_products table by selecting relevant columns from products_d
            connection.execute(text("""
                CREATE TABLE dim_products AS
                SELECT product_id AS ProductID, product_category AS ProductCategory,
                       product_name_lenght AS ProductNameLength, product_description_lenght AS ProductDescriptionLength,
                       product_photos_qty AS ProductPhotosQuantity, product_weight_g AS ProductWeightInGrams,
                       product_length_cm AS ProductLengthInCm, product_height_cm AS ProductHeightInCm,
                       product_width_cm AS ProductWidthInCm
                FROM products_d;
            """))
            
            # Modify ProductID to VARCHAR(255) and set it as the primary key
            connection.execute(text("ALTER TABLE dim_products MODIFY COLUMN ProductID VARCHAR(255);"))
            connection.execute(text("ALTER TABLE dim_products ADD PRIMARY KEY (ProductID);"))
        
        logging.info("Dimension Table 'dim_products' created successfully.")
    except Exception as e:
        logging.error(f"Error creating dim_products: {e}")
        raise

# Function to create a sellers dimension table with a primary key
def create_dim_sellers(engine):
    """
    Creates the dim_sellers table with SellerID as the primary key.
    """
    try:
        with engine.begin() as connection:
            # Drop the existing dim_sellers table if it exists
            connection.execute(text("DROP TABLE IF EXISTS dim_sellers;"))
            
            # Create the dim_sellers table by selecting relevant columns from sellers_d
            connection.execute(text("""
                CREATE TABLE dim_sellers AS
                SELECT seller_id AS SellerID, seller_city AS SellerCity, seller_state AS SellerState,
                       seller_zip_code AS SellerZIPCode
                FROM sellers_d;
            """))
            
            # Modify SellerID to VARCHAR(255) and set it as the primary key
            connection.execute(text("ALTER TABLE dim_sellers MODIFY COLUMN SellerID VARCHAR(255);"))
            connection.execute(text("ALTER TABLE dim_sellers ADD PRIMARY KEY (SellerID);"))
        
        logging.info("Dimension Table 'dim_sellers' created successfully.")
    except Exception as e:
        logging.error(f"Error creating dim_sellers: {e}")
        raise

def create_fact_order_items(engine):
    """
    Creates the fact_order_items table with foreign key constraints.
    """
    try:
        with engine.begin() as connection:
            # Drop the existing fact_order_items table if it exists
            connection.execute(text("DROP TABLE IF EXISTS fact_order_items;"))
            
            # Create the fact_order_items table
            connection.execute(text("""
                CREATE TABLE fact_order_items (
                    OrderID VARCHAR(255),
                    UserID VARCHAR(255),
                    ProductID VARCHAR(255),
                    SellerID VARCHAR(255),
                    PaymentID VARCHAR(255),
                    FeedbackID VARCHAR(255),
                    OrderDateKey BIGINT,
                    OrderTimeKey BIGINT,
                    PaymentValue DOUBLE,
                    UserState VARCHAR(70),
                    DeliveredDateKey BIGINT,
                    DeliveredTimeKey BIGINT,
                    DeliveryDelayCheck VARCHAR(6),
                    DeliveryDelayDays INT,
                    EstimatedDeliveryDateKey BIGINT,
                    EstimatedDeliveryTimeKey BIGINT,
                    OrderApprovedDateKey BIGINT,
                    OrderApprovedTimeKey BIGINT,
                    OrderStatus VARCHAR(20),
                    PickupDateKey BIGINT,
                    PickupTimeKey BIGINT,
                    Quantity INT,
                    ShippingDays INT,
                    FOREIGN KEY (UserID) REFERENCES dim_users(UserID),
                    FOREIGN KEY (ProductID) REFERENCES dim_products(ProductID),
                    FOREIGN KEY (SellerID) REFERENCES dim_sellers(SellerID),
                    FOREIGN KEY (PaymentID) REFERENCES dim_payments(PaymentID),
                    FOREIGN KEY (FeedbackID) REFERENCES dim_feedbacks(FeedbackID),
                    FOREIGN KEY (OrderDateKey) REFERENCES date_d(DateKey),
                    FOREIGN KEY (OrderTimeKey) REFERENCES times_d(TimeKey),
                    FOREIGN KEY (DeliveredDateKey) REFERENCES date_d(DateKey),
                    FOREIGN KEY (DeliveredTimeKey) REFERENCES times_d(TimeKey),
                    FOREIGN KEY (EstimatedDeliveryDateKey) REFERENCES date_d(DateKey),
                    FOREIGN KEY (EstimatedDeliveryTimeKey) REFERENCES times_d(TimeKey),
                    FOREIGN KEY (OrderApprovedDateKey) REFERENCES date_d(DateKey),
                    FOREIGN KEY (OrderApprovedTimeKey) REFERENCES times_d(TimeKey),
                    FOREIGN KEY (PickupDateKey) REFERENCES date_d(DateKey),
                    FOREIGN KEY (PickupTimeKey) REFERENCES times_d(TimeKey)
                );
            """))
        
        logging.info("Fact Table 'fact_order_items' created successfully.")
    except Exception as e:
        logging.error(f"Error creating fact_order_items: {e}")
        raise

# Function to populate the fact_order_items table
def insert_into_fact_order_items(engine):
    """
    Populates the fact_order_items table by joining transformed tables.
    """
    try:
        with engine.begin() as connection:
            connection.execute(text("""
                INSERT INTO fact_order_items (
                    OrderID, UserID, ProductID, SellerID, PaymentID, FeedbackID, 
                    OrderDateKey, OrderTimeKey, PaymentValue, UserState, 
                    DeliveredDateKey, DeliveredTimeKey, DeliveryDelayCheck, DeliveryDelayDays,
                    EstimatedDeliveryDateKey, EstimatedDeliveryTimeKey,
                    OrderApprovedDateKey, OrderApprovedTimeKey, 
                    OrderStatus, PickupDateKey, PickupTimeKey, 
                    Quantity, ShippingDays
                )
                SELECT
                    o.order_id AS OrderID,                          -- From orders_d
                    o.user_id AS UserID,                            -- From orders_d
                    oi.product_id AS ProductID,                     -- From order_item_d
                    oi.seller_id AS SellerID,                       -- From order_item_d
                    p.PaymentID,                                    -- From dim_payments
                    f.FeedbackID,                                   -- From dim_feedbacks
                    d1.DateKey AS OrderDateKey,                     -- From date_d (Order Date)
                    t1.TimeKey AS OrderTimeKey,                     -- From times_d (Order Time)
                    p.PaymentValue,                                 -- From dim_payments
                    u.UserState AS UserState,                       -- From dim_users
                    d2.DateKey AS DeliveredDateKey,                 -- From date_d (Delivered Date)
                    t2.TimeKey AS DeliveredTimeKey,                 -- From times_d (Delivered Time)
                    CASE 
                        WHEN d2.DateKey > d3.DateKey THEN 'TRUE' 
                        ELSE 'FALSE' 
                    END AS DeliveryDelayCheck,                      -- Derived logic
                    CASE 
                        WHEN DATEDIFF(d2.Date, d3.Date) < 0 THEN 0   
                        ELSE DATEDIFF(d2.Date, d3.Date) 
                    END AS DeliveryDelayDays,                       -- Derived logic
                    d3.DateKey AS EstimatedDeliveryDateKey,         -- From date_d (Estimated Delivery Date)
                    t3.TimeKey AS EstimatedDeliveryTimeKey,         -- From times_d (Estimated Delivery Time)
                    d4.DateKey AS OrderApprovedDateKey,             -- From date_d (Order Approved Date)
                    t4.TimeKey AS OrderApprovedTimeKey,             -- From times_d (Order Approved Time)
                    o.order_status AS OrderStatus,                  -- From orders_d
                    d5.DateKey AS PickupDateKey,                    -- From date_d (Pickup Date)
                    t5.TimeKey AS PickupTimeKey,                    -- From times_d (Pickup Time)
                    oi.quantity AS Quantity,                        -- From order_item_d
                    DATEDIFF(o.delivered_date, o.pickup_date) AS ShippingDays -- Derived logic
                FROM orders_d o
                LEFT JOIN order_item_d oi ON o.order_id = oi.order_id
                LEFT JOIN dim_users u ON o.user_id = u.UserID
                LEFT JOIN dim_sellers s ON oi.seller_id = s.SellerID
                LEFT JOIN dim_products pd ON oi.product_id = pd.ProductID
                LEFT JOIN dim_payments p ON o.order_id = p.PaymentID
                LEFT JOIN dim_feedbacks f ON o.feedback_id = f.FeedbackID
                LEFT JOIN date_d d1 ON DATE(o.order_date) = DATE(d1.Date)
                LEFT JOIN times_d t1 ON HOUR(o.order_date) = t1.Hour AND MINUTE(o.order_date) = t1.Minute
                LEFT JOIN date_d d2 ON DATE(o.delivered_date) = DATE(d2.Date)
                LEFT JOIN times_d t2 ON HOUR(o.delivered_date) = t2.Hour AND MINUTE(o.delivered_date) = t2.Minute
                LEFT JOIN date_d d3 ON DATE(o.estimated_time_delivery) = DATE(d3.Date)
                LEFT JOIN times_d t3 ON HOUR(o.estimated_time_delivery) = t3.Hour AND MINUTE(o.estimated_time_delivery) = t3.Minute
                LEFT JOIN date_d d4 ON DATE(o.order_approved_date) = DATE(d4.Date)
                LEFT JOIN times_d t4 ON HOUR(o.order_approved_date) = t4.Hour AND MINUTE(o.order_approved_date) = t4.Minute
                LEFT JOIN date_d d5 ON DATE(o.pickup_date) = DATE(d5.Date)
                LEFT JOIN times_d t5 ON HOUR(o.pickup_date) = t5.Hour AND MINUTE(o.pickup_date) = t5.Minute;
            """))
        logging.info("Fact table 'fact_order_items' populated successfully.")
    except Exception as e:
        logging.error(f"Error inserting into fact_order_items: {e}")
        raise
        
       

# Main function
def main():
    try:
        logging.info("Starting the dimension and fact table creation process...")
        
        # Create dimension tables
        create_dim_users(transform_engine)
        create_dim_feedbacks(transform_engine)
        create_dim_payments(transform_engine)
        create_dim_products(transform_engine)
        create_dim_sellers(transform_engine)
        
        # Create fact table
        create_fact_order_items(transform_engine)
        
        # Populate fact table
        insert_into_fact_order_items(transform_engine)
        
        logging.info("Dimension and fact table creation process completed successfully.")
    except Exception as e:
        logging.error(f"An error occurred during the process: {e}")
        raise

if __name__ == "__main__":
    main()