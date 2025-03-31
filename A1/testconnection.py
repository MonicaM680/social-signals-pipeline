from sqlalchemy import create_engine
from urllib.parse import quote_plus

# MySQL connection details
MYSQL_HOST = "127.0.0.1"
MYSQL_PORT = 3306
MYSQL_USER = "root"
MYSQL_PASSWORD = "home@123"  # Your password with special characters
MYSQL_DATABASE = "staging"

# URL-encode the password to handle special characters like '@'
encoded_password = quote_plus(MYSQL_PASSWORD)

# Create a SQLAlchemy engine with the encoded password
engine = create_engine(
    f"mysql+pymysql://{MYSQL_USER}:{encoded_password}@{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DATABASE}"
)

# Test the connection
try:
    with engine.connect() as connection:
        print("Connection successful!")
except Exception as e:
    print(f"Error connecting to MySQL: {e}")