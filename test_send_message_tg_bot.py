import asyncio

from main import write_telegram_message

if __name__ == "__main__":
    test_listing = {
        "listing_id": 123456,
        "number_rooms": 3,
        "size_qm": 75.0,
        "base_rent": 1200.0,
        "has_balkony": True,
        "address": "Some Address",
        "district": "Friedrichshain",
        "weblink": "https://example.com",
    }
    asyncio.run(write_telegram_message([test_listing]))
