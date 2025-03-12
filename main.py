"""
This script scrapes the appartment website inberlinwohnen/wohnungsfinder
for new listings and sends a message to a Telegram user if a new listing
matches the criteria defined in the script.
"""

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
import yaml
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

MINIMAL_SIZE = 56
MAXIMAL_BASE_RENT = 800
HAS_BALKONY = True
try:
    with open("forbidden_districts.yaml", encoding="utf-8") as f:
        FORBIDDEN_DISTRICTS = yaml.safe_load(f)["forbidden_districts"]
except (yaml.YAMLError, KeyError) as e:
    logging.error("Error loading forbidden districts: %s", str(e))
    FORBIDDEN_DISTRICTS = []

FLAT_ELEMENT = "//li[contains(@class, 'tb-merkflat')]"
BALKONY_XPATH = (
    ".//span[contains(@class, 'hackerl') and text()='Balkon/Loggia/Terrasse']"
)
LINK_XPATH = ".//a[@class='org-but']"


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


def get_listing_details(
    listing: WebElement,
) -> Dict[str, Any]:
    """Extract details from a listing element."""

    def _remove_thousand_separator(value: str) -> str:
        return value.replace(".", "")

    def _convert_decimal_separator(value: str) -> str:
        return value.replace(",", ".")

    text_items = re.split(r", |\| ", listing.text)
    if len(text_items) < 4:
        logging.warning("Skipping invalid listing: %s", listing.text)
        return {}

    listing_id = listing.get_attribute("id")
    rooms = float(_convert_decimal_separator(text_items[0].split(" ")[0]))
    size = float(_convert_decimal_separator(text_items[1].split(" ")[0]))
    base_rent = float(
        _convert_decimal_separator(
            _remove_thousand_separator(text_items[2].split(" ")[0])
        )
    )
    address = text_items[3]
    location = text_items[-1] if len(text_items) == 5 else "Unknown"
    balkony = bool(listing.find_elements("xpath", BALKONY_XPATH))
    link = listing.find_element("xpath", LINK_XPATH).get_attribute("href")
    wbs_required = bool(
        listing.find_elements(By.XPATH, ".//a[@title='Wohnberechtigungsschein']")
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
        "wbs_required": wbs_required,
    }


def get_listings() -> List[Dict[str, Any]]:
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


def save_listings_to_csv(listings: List[Dict[str, Any]]) -> None:
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
    interesting_listings: List[Dict[str, Any]],
) -> None:
    """Write a message to a Telegram user."""

    bot_token: str = environ.get("BOT_TOKEN", "")
    user_ids = json.loads(os.environ["USER_IDS"])

    def _maps_link(address: Any) -> str:
        return f"https://www.google.com/maps/search/?api=1&query={address.replace(' ', '+')}"

    def _assemble_message(listing: Dict[str, Any]) -> str:
        address = listing.get("address", "N/A")
        return (
            f"New Interesting Listing: \n\n"
            f"Listing ID: {listing.get('listing_id', 'N/A')}\n"
            f"Rooms: {listing.get('number_rooms', 'N/A')}\n"
            f"Size: {listing.get('size_qm', 'N/A')} m²\n"
            f"Base Rent: €{listing.get('base_rent', 'N/A')}\n"
            f"Balcony: {'Yes' if listing.get('has_balkony', False) else 'No'}\n"
            f"Address: <a href='{_maps_link(address)}'>{listing.get(address)}</a>\n"
            f"District: {listing.get('district', 'N/A')}\n"
            f"Link: {listing.get('weblink', 'N/A')}\n\n"
        )

    if len(interesting_listings) < 5:
        messages = [_assemble_message(listing) for listing in interesting_listings]
    else:
        messages = [_assemble_message(listing) for listing in interesting_listings[:5]]
        messages.append(
            f"and {len(interesting_listings) - 5} more listings were truncated."
        )

    try:
        message = "\n".join(messages)
        bot = Bot(token=bot_token)
        for user_id in user_ids:
            await bot.send_message(chat_id=user_id, text=message, parse_mode="HTML")

        logging.info("Message sent successfully!")
    except (telegram.error.TelegramError, ValueError) as e:
        logging.error("Failed to send message: %s", str(e))


def monitor_changes(sleep_interval: int = 300):
    """Monitor changes in listings and save new to CSV."""

    while True:
        old_listings = get_listings_from_csv()
        current_listings = get_listings()
        if not current_listings:
            logging.warning("No listings found. Retrying...")
            continue

        old_listing_ids = [listing["listing_id"] for listing in old_listings]
        new_listings = [
            listing
            for listing in current_listings
            if listing["listing_id"] not in old_listing_ids
        ]

        if new_listings:
            logging.info(
                "New listings found: %s",
                [listing["listing_id"] for listing in new_listings],
            )
            save_listings_to_csv(new_listings)

            new_relevant_listings = []

            for new_listing in new_listings:
                size_criterion = float(new_listing["size_qm"]) >= MINIMAL_SIZE
                base_rent_criterion = (
                    float(new_listing["base_rent"]) <= MAXIMAL_BASE_RENT
                )
                district_criterion = new_listing["district"] not in FORBIDDEN_DISTRICTS
                balkony_criterion = new_listing["has_balkony"] == HAS_BALKONY
                wbs_criterion = (
                    not new_listing["wbs_required"] or new_listing["number_rooms"] <= 2
                )

                if (
                    size_criterion
                    and base_rent_criterion
                    and balkony_criterion
                    and district_criterion
                    and wbs_criterion
                ):
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

    interval = random.randint(28, 142)
    monitor_changes(interval)
