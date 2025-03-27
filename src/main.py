from scrapers.gallito import PropertyScraper
from settings.settings import load_settings
from settings.logger import custom_logger


logger = custom_logger("Main")


if __name__ == "__main__":

    # Set the settings
    settings = load_settings(key="WebPage")
    BASE_URL = settings["BaseUrl"]
    VALIDATION_URL = settings["ValidationUrl"]
    AMOUNT_OF_PROPERTIES = settings["AmountOfProperties"]

    # Initialize the scraper
    scraper = PropertyScraper(
        max_properties=AMOUNT_OF_PROPERTIES,
    )

    # Run the scraper
    scraper.run(
        base_url=BASE_URL,
        validation_url=VALIDATION_URL,
    )
