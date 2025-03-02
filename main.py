import asyncio
import csv
import json
import logging
import os
import random
import re
import time
from os import environ
from typing import Any, Dict, List, Union

import telegram
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.remote.webelement import WebElement
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from telegram import Bot
from webdriver_manager.chrome import ChromeDriverManager

URL = "https://inberlinwohnen.de/wohnungsfinder/"
CSV_FILE = "listings.csv"

# Apartment filters
MINIMAL_SIZE = 55
MAXIMAL_BASE_RENT = 700
HAS_BALKONY = True

# listing web element
FLAT_ELEMENT = "//li[contains(@class, 'tb-merkflat')]"
BALKONY_XPATH = (
    ".//span[contains(@class, 'hackerl') and text()='Balkon/Loggia/Terrasse']"
)
LINK_XPATH = ".//a[@class='org-but']"


# Logger
def setup_logger() -> None:
    """Setup the logger."""

    logging.basicConfig(
        format="%(asctime)s - %(message)s",
        level=logging.INFO,
    )

    logging.getLogger("selenium").setLevel(logging.WARNING)


def log_last_new_appartment() -> None:
    """Log the time since the last update of listings."""

    current_time = time.time()

    if os.path.exists(CSV_FILE):
        elapsed_time_since_last_fetch = current_time - time.mktime(
            time.localtime(os.path.getmtime(CSV_FILE))
        )
    else:
        elapsed_time_since_last_fetch = 0

    logging.info(
        "Fetched listings. Time since last update: %d minutes",
        round(elapsed_time_since_last_fetch / 60),
    )


def get_driver() -> webdriver.Chrome:
    """Get a headless Chrome driver."""

    options = Options()
    options.add_argument("--headless")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")

    driver = webdriver.Chrome(
        service=Service(ChromeDriverManager().install()), options=options
    )

    return driver


def parse_float(value: str) -> float:
    """Helper function to parse floats with commas."""

    try:
        return float(value.replace(",", "."))
    except ValueError:
        return 0.0


def get_listing_details(
    listing: WebElement,
) -> Dict[str, Union[float, str, bool, None, Any]]:
    """Extract details from a listing element."""

    text = re.split(r", |\| ", listing.text)
    if len(text) <= 2:
        logging.warning("Skipping invalid listing: %s", listing.text)
        return {}

    listing_id = listing.get_attribute("id")
    rooms = parse_float(re.findall(r"\d+", text[0])[0])
    size_match = re.search(r"(\d+),(\d+)", text[1])
    base_rent_match = re.search(r"(\d+),(\d+)", text[2])
    address = text[3]
    location = text[-1] if len(text) == 5 else "Unknown"
    balkony = bool(listing.find_elements("xpath", BALKONY_XPATH))
    link = listing.find_element("xpath", LINK_XPATH).get_attribute("href")

    if size_match and base_rent_match:
        size = parse_float(f"{size_match.group(1)}.{size_match.group(2)}")
        base_rent = parse_float(
            f"{base_rent_match.group(1)}.{base_rent_match.group(2)}"
        )

        return {
            "listing_id": listing_id,
            "number_rooms": rooms,
            "size_qm": size,
            "base_rent": base_rent,
            "address": address,
            "district": location,
            "has_balkony": balkony,
            "weblink": link,
        }

    logging.warning("Skipping invalid listing: %s", listing.text)
    return {}


def get_listings() -> List[Dict[str, Union[float, str, bool]]]:
    """Scrape listings from the website and return structured data."""

    driver = get_driver()
    driver.get(URL)

    WebDriverWait(driver, 16).until(
        EC.presence_of_element_located((By.XPATH, FLAT_ELEMENT))
    )

    listings = driver.find_elements("xpath", FLAT_ELEMENT)
    structured_listings = []

    for listing in listings:
        try:
            details = get_listing_details(listing)
            structured_listings.append(details)
        except (ValueError, AttributeError, IndexError) as e:
            logging.error("Error processing listing: %s", str(e))

    driver.quit()
    log_last_new_appartment()
    return structured_listings


def save_listings_to_csv(listings: List[Dict[str, Union[float, str, bool]]]) -> None:
    """Save new listings to a CSV file."""

    file_exists = os.path.isfile(CSV_FILE)
    fieldnames = list(listings[0].keys()) + ["timestamp"]

    try:
        with open(CSV_FILE, mode="a", newline="", encoding="utf-8-sig") as file:
            writer = csv.DictWriter(file, fieldnames=fieldnames)

            if not file_exists:
                writer.writeheader()

            timestamp = time.strftime("%Y-%m-%d %H:%M:%S")
            for listing in listings:
                listing["timestamp"] = timestamp
                writer.writerow(listing)
    except (IOError, csv.Error) as e:
        logging.error("Error saving listings to CSV: %s", str(e))


def get_listings_from_csv() -> List[Dict[str, Union[str, float, bool]]]:
    """Get listings from the CSV file."""

    listings = []

    if os.path.isfile(CSV_FILE):
        try:
            with open(CSV_FILE, mode="r", encoding="utf-8-sig") as file:
                listings = list(csv.DictReader(file))
        except (IOError, csv.Error) as e:
            logging.error("Error reading CSV file: %s", str(e))
    return listings


async def write_telegram_message(
    interesting_listings: List[Dict[str, Union[str, float, bool]]]
) -> None:
    """Write a message to a Telegram user."""

    bot_token: str = environ.get("BOT_TOKEN", "")
    user_ids = json.loads(os.environ["USER_IDS"])

    messages = [
        (
            f"New Interesting Listing: \n\n"
            f"Listing ID: {interesting_listing.get('listing_id', 'N/A')}\n"
            f"Rooms: {interesting_listing.get('number_rooms', 'N/A')}\n"
            f"Size [m2]: {interesting_listing.get('size_qm', 'N/A')} m²\n"
            f"Base Rent [EUR]: €{interesting_listing.get('base_rent', 'N/A')}\n"
            f"Balcony: {'Yes' if interesting_listing.get('has_balkony', False) else 'No'}\n"
            f"Address: {interesting_listing.get('address', 'N/A')}\n"
            f"District: {interesting_listing.get('district', 'N/A')}\n"
            f"Link: {interesting_listing.get('weblink', 'N/A')}\n\n"
        )
        for interesting_listing in interesting_listings
    ]

    try:
        message = "\n".join(messages)
        bot = Bot(token=bot_token)
        for user_id in user_ids:
            await bot.send_message(chat_id=user_id, text=message)

        logging.info("Message sent successfully!")
    except (telegram.error.TelegramError, ValueError) as e:
        logging.error("Failed to send message: %s", str(e))


def monitor_changes(sleep_interval: int = 300):
    """Monitor changes in listings and save new ones to CSV."""

    while True:
        old_listings = get_listings_from_csv()
        new_listings = get_listings()
        if not new_listings:
            logging.warning("No listings found. Retrying...")
            continue

        old_listing_ids = [listing["listing_id"] for listing in old_listings]
        new_unique_listings = [
            listing
            for listing in new_listings
            if listing["listing_id"] not in old_listing_ids
        ]

        if new_unique_listings:
            logging.info("New listings found: %s", new_unique_listings)
            save_listings_to_csv(new_unique_listings)

            new_relevant_listings = []

            for new_listing in new_unique_listings:
                size_criterion = float(new_listing["size_qm"]) >= MINIMAL_SIZE
                base_rent_criterion = (
                    float(new_listing["base_rent"]) <= MAXIMAL_BASE_RENT
                )
                balkony_criterion = new_listing["has_balkony"] == HAS_BALKONY
                if size_criterion and base_rent_criterion and balkony_criterion:
                    new_relevant_listings.append(new_listing)

            asyncio.run(write_telegram_message(new_relevant_listings))
            time.sleep(5)

        time.sleep(sleep_interval)


if __name__ == "__main__":
    setup_logger()
    if not os.path.isfile(CSV_FILE):
        first_time_listings = get_listings()
        if first_time_listings:
            save_listings_to_csv(first_time_listings)

    interval = random.randint(28, 242)
    monitor_changes(interval)
