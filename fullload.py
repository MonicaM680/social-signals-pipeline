import os
import pandas as pd
from sqlalchemy import create_engine, MetaData, Table, Column, Integer, Float, String, DateTime, Text
from sqlalchemy.exc import SQLAlchemyError
from urllib.parse import quote_plus

# Define the directory path where the CSV files are located
directory_path = "/Users/monicamuniraj/Desktop/BP/social-signals-pipeline/Dataset"

# MySQL connection details
MYSQL_HOST = "127.0.0.1"
MYSQL_PORT = 3306
MYSQL_USER = "root"
MYSQL_PASSWORD = "home@123"  # Password with special characters
MYSQL_DATABASE = "staging"  # Database name

# URL-encode the password to handle special characters like '@'
encoded_password = quote_plus(MYSQL_PASSWORD)

# Create an engine to connect to MySQL without specifying a database
engine_init = create_engine(f"mysql+pymysql://{MYSQL_USER}:{encoded_password}@{MYSQL_HOST}:{MYSQL_PORT}")

# Create the database if it does not exist
with engine_init.connect() as conn:
    conn.execute(f"CREATE DATABASE IF NOT EXISTS {MYSQL_DATABASE}")
    print(f"Database `{MYSQL_DATABASE}` checked/created successfully.")

# Create a SQLAlchemy engine
engine = create_engine(
    f"mysql+pymysql://{MYSQL_USER}:{encoded_password}@{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DATABASE}"
)

# Function to determine if a column contains only whole numbers
def is_whole_number(column_data):
    try:
        # Attempt to convert the column to numeric and check if all values are whole numbers
        numeric_data = pd.to_numeric(column_data, errors='coerce')
        return (numeric_data.dropna() % 1 == 0).all()
    except Exception:
        return False

# Function to map Pandas data types to SQLAlchemy column types
def map_dtype_to_sqlalchemy(dtype, column_data):
    if dtype == 'int64':
        return Integer()
    elif dtype == 'float64':
        return Float()
    elif dtype == 'datetime64[ns]':
        return DateTime()
    else:
        # Default to VARCHAR(255) for all other types
        return String(255)

# Function to preprocess and infer schema
def preprocess_and_infer_schema(df):
    schema = {}
    for col in df.columns:
        # Check if the column can be converted to numeric
        try:
            numeric_data = pd.to_numeric(df[col], errors='coerce')
            
            # If at least one value is numeric, determine if it's an integer or float
            if not numeric_data.isna().all():
                if is_whole_number(numeric_data):
                    df[col] = numeric_data.fillna(0).astype('int64')  # Fill NaN with 0 and convert to int
                    schema[col] = Integer()
                else:
                    df[col] = numeric_data.fillna(0.0).astype('float64')  # Fill NaN with 0.0 and convert to float
                    schema[col] = Float()
                continue
        except Exception:
            pass
        
        # Check if the column contains date-like values
        try:
            # Let pandas infer the datetime format automatically
            datetime_data = pd.to_datetime(df[col], errors='coerce')
            if not datetime_data.isna().all():  # If at least one value is a valid datetime
                df[col] = datetime_data  # Leave missing values as NaT
                schema[col] = DateTime()
                continue
        except Exception:
            pass
        
        # Default to VARCHAR(255) for all other cases
        df[col] = df[col].fillna('')  # Fill missing values with an empty string for non-numeric columns
        schema[col] = String(255)
    
    return df, schema

# Function to create a table in the database
def create_table(engine, table_name, schema):
    metadata = MetaData()
    
    # Define the table structure using the inferred schema
    columns = [Column(col, dtype) for col, dtype in schema.items()]
    table = Table(table_name, metadata, *columns)
    
    try:
        # Drop the table if it already exists
        if engine.dialect.has_table(engine.connect(), table_name):
            table.drop(engine)
            print(f"Dropped existing table: {table_name}")
        
        # Create the table
        metadata.create_all(engine)
        print(f"Created table: {table_name}")
    except SQLAlchemyError as e:
        print(f"Error creating table `{table_name}`: {e}")

# Function to load data into a table
def load_data(engine, table_name, df):
    try:
        # Insert data into the table
        df.to_sql(table_name, con=engine, if_exists='append', index=False)
        print(f"Data loaded into table: {table_name}")
    except SQLAlchemyError as e:
        print(f"Error loading data into table `{table_name}`: {e}")

# Main function to process all CSV files
def process_csv_files(directory_path):
    # Iterate through all CSV files in the directory
    for csv_file in os.listdir(directory_path):
        if csv_file.endswith(".csv"):
            file_path = os.path.join(directory_path, csv_file)
            table_name = os.path.splitext(csv_file)[0]  # Use the file name as the table name
            
            print(f"\nProcessing file: {csv_file}")
            
            # Read the CSV file with all columns as strings
            df = pd.read_csv(file_path, dtype=str)
            
            # Preprocess and infer schema
            df, schema = preprocess_and_infer_schema(df)
            
            # Print the inferred schema
            print(f"Inferred Schema for `{table_name}`:")
            for col, dtype in schema.items():
                print(f"  {col}: {dtype.__class__.__name__}")
            
            # Create the table in the database
            create_table(engine, table_name, schema)
            
            # Load data into the table
            load_data(engine, table_name, df)

# Call the main function
process_csv_files(directory_path)