import requests
import csv
import logging
import os

# Configure logging
logging.basicConfig(
    filename="api_script.log",  
    level=logging.INFO,        
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# Base URL of the API
BASE_URL = "https://potterapi-fedeperin.vercel.app/en"

# Directory to store CSV files
DATA_FOLDER = "hp_data"

# Create the directory if it doesn't exist
if not os.path.exists(DATA_FOLDER):
    os.makedirs(DATA_FOLDER)
    logging.info(f"Created directory: {DATA_FOLDER}")

def get_data(endpoint):
    """
    Fetch data from the specified API endpoint.
    :param endpoint: The API endpoint (e.g., 'characters', 'houses', etc.)
    :return: JSON response if successful, None otherwise
    """
    try:
        response = requests.get(f"{BASE_URL}/{endpoint}")
        response.raise_for_status()  # Raise an exception for HTTP errors
        return response.json()
    except requests.exceptions.RequestException as e:
        logging.error(f"Error fetching {endpoint}: {e}")
        return None

def save_to_csv(data, filename, headers):
    """
    Save data to a CSV file.
    :param data: List of dictionaries containing the data
    :param filename: Name of the CSV file
    :param headers: List of column headers
    """
    filepath = os.path.join(DATA_FOLDER, filename)  
    try:
        with open(filepath, mode="w", newline="", encoding="utf-8") as file:
            writer = csv.DictWriter(file, fieldnames=headers)
            writer.writeheader()  # Write the header row
            writer.writerows(data)  # Write the data rows
        logging.info(f"Data successfully saved to {filepath}")
    except Exception as e:
        logging.error(f"Error saving data to {filepath}: {e}")

if __name__ == "__main__":
    # Fetch data from the API
    characters = get_data("characters")
    houses = get_data("houses")
    spells = get_data("spells")
    books = get_data("books")

    # Save characters to CSV
    if characters:
        save_to_csv(
            data=[
                {
                    "full_name": character.get("fullName", "Unknown"),
                    "nickname": character.get("nickname", "Unknown"),
                    "hogwarts_house": character.get("hogwartsHouse", "Unknown"),
                    "interpreted_by": character.get("interpretedBy", "Unknown"),
                    "children": ", ".join(character.get("children", ["Unknown"])),
                    "image": character.get("image", "No image available"),
                    "birthdate": character.get("birthdate", "Unknown"),
                    "index": character.get("index", "Unknown")
                }
                for character in characters
            ],
            filename="characters.csv",
            headers=[
                "full_name", "nickname", "hogwarts_house", "interpreted_by",
                "children", "image", "birthdate", "index"
            ]
        )

    # Save houses to CSV
    if houses:
        save_to_csv(
            data=[
                {
                    "house": house.get("house", "Unknown"),
                    "emoji": house.get("emoji", "Unknown"),
                    "founder": house.get("founder", "Unknown"),
                    "colors": ", ".join(house.get("colors", ["Unknown"])),
                    "animal": house.get("animal", "Unknown"),
                    "index": house.get("index", "Unknown")
                }
                for house in houses
            ],
            filename="houses.csv",
            headers=["house", "emoji", "founder", "colors", "animal", "index"]
        )

    # Save spells to CSV
    if spells:
        save_to_csv(
            data=[
                {
                    "spell": spell.get("spell", "Unknown"),
                    "use": spell.get("use", "Unknown"),
                    "index": spell.get("index", "Unknown")
                }
                for spell in spells
            ],
            filename="spells.csv",
            headers=["spell", "use", "index"]
        )

    # Save books to CSV
    if books:
        save_to_csv(  
            data=[
                {
                    "title": book.get("title", "Unknown Title"),
                    "release_date": book.get("releaseDate", "Unknown Release Date"),
                    "description": book.get("description", "No description available"),
                    "pages": book.get("pages", "Unknown"),
                    "cover_image": book.get("cover", "No cover image available"),
                    "book_number": book.get("number", "Unknown"),
                    "index": book.get("index", "Unknown")
                }
                for book in books
            ],
            filename="books.csv",
            headers=[
                "title", "release_date", "description", "pages", "cover_image", "book_number", "index"
            ]
        )