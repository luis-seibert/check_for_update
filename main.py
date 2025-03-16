"""
This script scrapes the website in FLAT_FINDER_URL for new listings and sends a message
to Telegram users when new listings match the criteria defined in the filter options.
"""

import asyncio
import csv
import json
import os
import random
import re
import time
from os import environ
from typing import Any

import requests
import telegram
import yaml
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.remote.webelement import WebElement
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from telegram import Bot
from webdriver_manager.chrome import ChromeDriverManager
from logger import Logger

FLAT_FINDER_URL = os.getenv("FLAT_FINDER_URL")
FLAT_ELEMENT = os.getenv("FLAT_ELEMENT")
BALKONY_XPATH = os.getenv("BALKONY_XPATH")
LINK_XPATH = os.getenv("LINK_XPATH")
WBS_XPATH = os.getenv("WBS_XPATH")

MINIMAL_SIZE = float(os.getenv("MINIMAL_SIZE"))
MAXIMAL_BASE_RENT = float(os.getenv("MAXIMAL_BASE_RENT"))
HAS_BALKONY = os.getenv("HAS_BALKONY")

DISTRICTS_YAML = "berlin_districts.yaml"
SCANNED_FLATS_CSV = "listings.csv"
UNKNOWN_OSM_ADRESSES = "unkown_osm_adresses.yaml"

logger = Logger()
try:
    with open("berlin_districts.yaml", encoding="utf-8") as f:
        FORBIDDEN_DISTRICTS = yaml.safe_load(f)[0]["forbidden_districts"]
except (yaml.YAMLError, KeyError) as e:
    logger.log_error(f"Error loading forbidden districts: {e}")
    FORBIDDEN_DISTRICTS = []


def main():
    """Main function to scrape listings and monitor changes."""

    logger.log_info("Starting the main function.")
    driver = get_driver()

    interval = random.randint(28, 142)
    monitor_changes(driver, interval)


def monitor_changes(driver: webdriver, sleep_interval: int = 300) -> None:
    """Monitor changes in listings and save new to CSV.

    Args:
        driver (webdriver): Selenium WebDriver instance to scrape listings.
        sleep_interval (int, optional): Time to sleep between checks. Defaults to 300.
    """

    logger.log_info("Starting to monitor changes in listings.")
    if not os.path.isfile(SCANNED_FLATS_CSV):
        first_time_listings = get_listings(driver)
        if first_time_listings:
            save_listings_to_csv(first_time_listings)

    while True:
        old_listings = get_listings_from_csv()
        current_listings = get_listings(driver)
        if not current_listings:
            logger.log_warning("No listings found. Retrying...")
            continue

        old_listing_ids = [listing["listing_id"] for listing in old_listings]
        new_listings = [
            listing
            for listing in current_listings
            if listing["listing_id"] not in old_listing_ids
        ]

        if new_listings:
            logger.log_info(
                f"New listings found: {[listing['listing_id'] for listing in new_listings]}"
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

            if new_relevant_listings:
                asyncio.run(write_telegram_message(new_relevant_listings))
                time.sleep(5)
        else:
            logger.log_info("No new listings found.")

        time.sleep(sleep_interval)


def get_driver() -> webdriver.Chrome:
    """Get a headless Chrome driver.

    Returns:
        webdriver.Chrome: Headless Chrome driver.
    """

    logger.log_info("Initializing Chrome driver.")
    options = Options()
    options.add_argument("--headless")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")

    driver = webdriver.Chrome(
        service=Service(ChromeDriverManager().install()), options=options
    )
    logger.log_info("Chrome driver initialized successfully.")

    return driver


def get_listings(driver: webdriver) -> list[dict[str, Any]]:
    """Scrape listings from the website and return structured data.

    Args:
        driver (webdriver): Selenium WebDriver instance to scrape listings.

    Returns:
        list[dict[str, Any]]: Structured data of listings.
    """

    logger.log_info("Fetching listings from website.")
    while True:
        try:
            driver.get(FLAT_FINDER_URL)
            WebDriverWait(driver, 16).until(
                EC.presence_of_element_located((By.XPATH, FLAT_ELEMENT))
            )
            break
        except TimeoutException as e:
            logger.log_error(f"Error fetching listings: {str(e)}")
            driver = get_driver()

    listings = driver.find_elements("xpath", FLAT_ELEMENT)

    structured_listings = []
    for listing in listings:
        try:
            details = get_listing_details(listing)
            structured_listings.append(details)
        except (ValueError, AttributeError, IndexError) as e:
            logger.log_error(f"Error processing listing: {str(e)}")

    logger.log_info(f"Fetched {len(structured_listings)} listings.")
    logger.log_last_new_appartment(SCANNED_FLATS_CSV)

    return structured_listings


def get_listing_details(listing: WebElement) -> dict[str, Any]:
    """Extract details from a listing element.

    Args:
        listing (WebElement): Listing element to extract details from.

    Returns:
        dict[str, Any]: Extracted details from the listing.
    """

    def _remove_thousand_separator(value: str) -> str:
        return value.replace(".", "")

    def _convert_decimal_separator(value: str) -> str:
        return value.replace(",", ".")

    text_items = re.split(r", |\| ", listing.text)
    if len(text_items) < 4:
        logger.log_warning(f"Skipping invalid listing: {listing.text}")
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
    balkony = bool(listing.find_elements("xpath", BALKONY_XPATH))
    link = listing.find_element("xpath", LINK_XPATH).get_attribute("href")
    wbs_required = bool(listing.find_elements(By.XPATH, WBS_XPATH))

    if len(text_items) == 5:
        location = text_items[-1]
    elif address:
        location = get_district_from_osm(address + " Berlin")
        time.sleep(1)
    else:
        location = "Unknown"

    flat_details = {
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
    return flat_details


def get_district_from_osm(address: str) -> tuple[str, list[str]]:
    """Get the district of an address from OpenStreetMap.

    Args:
        address (str): Address to get the district from.

    Returns:
        tuple[str, list[str]]: District of the address and updated cache.
    """

    def _save_invalid_addresses(unknown_addresses: list[str]) -> None:
        with open(
            UNKNOWN_OSM_ADRESSES, "r+", encoding="utf-8"
        ) as unknown_addresses_file:
            yaml.dump(unknown_addresses, unknown_addresses_file, allow_unicode=True)

    unknown_osm_addresses = get_unknown_osm_adresses()
    url = f"https://nominatim.openstreetmap.org/search?q={address}&format=json"
    headers = {"User-Agent": "GeoDecoder"}
    retry = 0

    if address not in unknown_osm_addresses:
        while retry <= 2:
            try:
                response = requests.get(url, headers=headers, timeout=30)
                response_json = response.json()
                osm_address = re.split(", ", response_json[0]["display_name"])
                osm_district = osm_address[-5]
                return osm_district

            except requests.exceptions.ReadTimeout:
                retry += 1
                logger.log_warning(f"Timeout error. Retrying {retry}/2.")
                time.sleep(15)
                continue

            except Exception as e:
                logger.log_exception(f"Error getting district from OSM: {str(e)}")
                unknown_osm_addresses.append(address)
                _save_invalid_addresses(unknown_osm_addresses)
                return "Unknown"

    return "Unknown"


def get_unknown_osm_adresses() -> list[str]:
    """Setup the cache for invalid OSM addresses.

    Returns:
        list[str]: list of invalid addresses.
    """

    def _create_invalid_adresses_file() -> list:
        with open(UNKNOWN_OSM_ADRESSES, "w", encoding="utf-8") as unkown_addresses_file:
            yaml.dump([], unkown_addresses_file, allow_unicode=True)
            return []

    def _load_invalid_addresses() -> list[str]:
        with open(UNKNOWN_OSM_ADRESSES, encoding="utf-8") as unkown_addresses_file:
            invalid_addresses = yaml.safe_load(unkown_addresses_file)
        if not invalid_addresses:
            return []
        return invalid_addresses

    if not os.path.isfile(UNKNOWN_OSM_ADRESSES):
        return _create_invalid_adresses_file()

    return _load_invalid_addresses()


def save_listings_to_csv(listings: list[dict[str, Any]]) -> None:
    """Save new listings to a CSV file.

    Args:
        listings (list[dict[str, Any]]): Listings to save to CSV.
    """

    logger.log_info("Saving listings to CSV.")
    file_exists = os.path.isfile(SCANNED_FLATS_CSV)
    fieldnames = list(listings[0].keys()) + ["timestamp"]

    try:
        with open(
            SCANNED_FLATS_CSV, mode="a", newline="", encoding="utf-8-sig"
        ) as file:
            writer = csv.DictWriter(file, fieldnames=fieldnames)

            if not file_exists:
                writer.writeheader()

            timestamp = time.strftime("%Y-%m-%d %H:%M:%S")
            for listing in listings:
                listing["timestamp"] = timestamp
                writer.writerow(listing)

    except (IOError, csv.Error) as e:
        logger.log_error(f"Error saving listings to CSV: {str(e)}")


def get_listings_from_csv() -> list[dict[str, str | float | bool]]:
    """Get listings from the CSV file.

    Returns:
        list[dict[str, list[dict[str, str | float | bool]]: List of flats from CSV.
    """

    logger.log_info("Reading listings from CSV.")
    listings = []
    if os.path.isfile(SCANNED_FLATS_CSV):
        try:
            with open(SCANNED_FLATS_CSV, mode="r", encoding="utf-8-sig") as file:
                listings = list(csv.DictReader(file))
        except (IOError, csv.Error) as e:
            logger.log_error(f"Error reading CSV file: {str(e)}")

    return listings


async def write_telegram_message(interesting_listings: list[dict[str, Any]]) -> None:
    """Write a message to a Telegram user.

    Args:
        interesting_listings (list[dict[str, Any]]): List of relevant listings.
    """

    bot_token: str = environ.get("BOT_TOKEN", "")
    user_ids = json.loads(os.environ["USER_IDS"])

    def _maps_link(address: str) -> str:
        return f"https://www.google.com/maps/search/?api=1&query={address.replace(' ', '+')}"

    def _assemble_message(listing: dict[str, Any]) -> str:
        address = listing.get("address", "N/A")
        return (
            f"New Interesting Listing: \n\n"
            f"Listing ID: {listing.get('listing_id', 'N/A')}\n"
            f"Rooms: {listing.get('number_rooms', 'N/A')}\n"
            f"Size: {listing.get('size_qm', 'N/A')} m²\n"
            f"Base Rent: €{listing.get('base_rent', 'N/A')}\n"
            f"Balcony: {'Yes' if listing.get('has_balkony', False) else 'No'}\n"
            f"Address: <a href='{_maps_link(address)}'>{address}</a>\n"
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
        logger.log_info("Messages sent successfully!")

    except (telegram.error.TelegramError, ValueError) as e:
        logger.log_error(f"Failed to send message: {str(e)}")


if __name__ == "__main__":
    main()
