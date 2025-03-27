import json
import os
import re
from typing import Dict, List
import sys

sys.path.append(os.getcwd())

from playwright.sync_api import Browser, Page, sync_playwright
import requests

from src.settings import custom_logger
from src.structs import PropertyType, Property, PropertyDetails, PropertyOperation


VALID_IMAGE_EXTENSIONS = [".jpg", ".jpeg", ".png", ".webp"]


class PropertyScraper:
    def __init__(
        self, output_dir: str = "data/scraped_data", max_properties: int = 60
    ) -> None:
        """
        Initialize the PropertyScraper

        Args:
            output_dir (str): The directory to store scraped data
            max_properties (int): The maximum number of properties to scrape
        """

        self.logger = custom_logger(self.__class__.__name__)

        # Set up directories for storing scraped data
        self.output_dir = output_dir
        self.images_dir = os.path.join(self.output_dir, "images")
        self.properties_dir = os.path.join(self.output_dir, "properties")

        # Create necessary directories if they don't exist
        os.makedirs(self.output_dir, exist_ok=True)
        os.makedirs(self.images_dir, exist_ok=True)
        os.makedirs(self.properties_dir, exist_ok=True)

        # Define possible property types
        self.possible_types = {
            "casa": PropertyType.HOUSE,
            "apartamento": PropertyType.APARTMENT,
        }

        # Keep track of processed properties to avoid duplicates
        self.processed_properties = set()
        self._load_processed_properties()

        # Initialize counters for processed properties
        self.properties_processed = len(self.processed_properties)
        self.max_properties = max_properties
        self.logger.info(
            "PropertyScraper initialized. Output directory: %s", self.output_dir
        )

    def _load_processed_properties(self) -> None:
        """Load already processed properties from existing JSONL files"""

        for filename in os.listdir(self.properties_dir):
            if filename.endswith(".jsonl"):
                property_id = filename.replace(".jsonl", "")
                self.processed_properties.add(property_id)
        self.logger.info(
            f"Found {len(self.processed_properties)} previously processed properties"
        )

    def run(self, base_url: str, validation_url: str) -> None:
        """
        Run the property scraper.

        Args:
            base_url (str): The base URL for property listings.
            validation_url (str): The URL to validate property links.
        """

        self.logger.info("Starting scraper run")

        # Initialize the browser
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            context = browser.new_context(
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
            )

            # Open a new page
            page = context.new_page()
            current_page = 1

            while True:
                # Check if the maximum number of properties has been reached
                if self.properties_processed >= self.max_properties:
                    self.logger.info(
                        f"Reached maximum number of new properties ({self.max_properties})"
                    )
                    break

                # Construct the URL for the current page
                page_url = f"{base_url}?pag={current_page}"
                self.logger.info(f"Processing page {current_page}, URL: {page_url}")

                # Navigate to the page
                page.goto(page_url)

                # Get property links on the current page
                property_links = self._get_property_links(page, validation_url)
                self.logger.info(
                    f"Found {len(property_links)} unique property links on page {current_page}"
                )

                # Process each property link found on the page
                self._process_properties(page, property_links, browser)

                # Check if there's a next page
                next_page_links = page.locator("#paginador a").all()
                next_page_exists = any(
                    link.text_content() == ">" for link in next_page_links
                )

                # If there are no more pages to process, break the loop
                if not next_page_exists:
                    self.logger.info("No more pages to process")
                    break

                current_page += 1

            self.logger.info("Finished processing all properties across all pages")
            browser.close()

    def _get_property_links(self, page: Page, validation_url: str) -> List[str]:
        """
        Get property links from the current page

        Args:
            page (Page): The page object to get property links from
            validation_url (str): The URL to validate property links

        Returns:
            List[str]: A list of property links
        """

        property_links = []

        # Find all links and filter for property listings (ending in 8 digits)
        all_links = page.locator("a").all()

        # Extract href attributes and filter for valid property links
        for link in all_links:
            href = link.get_attribute("href")

            # Only include links that end with 8 digits (property IDs)
            if (
                href
                and re.match(r".*-\d{8}$", href)
                and (href not in property_links)
                and href.startswith(validation_url)
            ):
                property_links.append(href)

        # Remove duplicates while preserving order
        property_links = list(dict.fromkeys(property_links))

        return property_links

    def _process_properties(
        self,
        page: Page,
        property_links: List[str],
        browser: Browser,
    ) -> None:
        """
        Process each property link found on the page

        Args:
            page (Page): The page object to process property links from
            property_links (List[str]): A list of property links to process
            browser (Browser): The browser object to use for processing
        """

        for i, link in enumerate(property_links, 1):
            try:
                #  Check if the property has already been processed
                property = Property(
                    id=link.split("-")[-1],
                    type=PropertyType.UNKNOWN,
                    link=link,
                    images=[],
                    details=None,
                )
                if property.id in self.processed_properties:
                    self.logger.info(
                        f"Skipping already processed property {property.id} ({i}/{len(property_links)})"
                    )
                    continue

                # Process the property
                self.logger.info(
                    f"Processing property {i}/{len(property_links)}: {link}"
                )
                self._process_property(page, property)

                # Mark the property as processed
                self.processed_properties.add(property.id)
                self.properties_processed += 1
                self.logger.info(
                    f"Processed {self.properties_processed}/{self.max_properties} properties"
                )

                # Check if the maximum number of properties has been reached
                if self.properties_processed >= self.max_properties:
                    break

            except Exception as e:
                self.logger.error(f"Error processing property {link}: {str(e)}")
                continue

    def _process_property(self, page: Page, property: Property) -> None:
        """
        Process a property by navigating to its URL and extracting its ID and image URLs

        Args:
            page (Page): The page object to use for processing
            property (Property): The property to process
        """

        # Navigate to the property URL
        self.logger.debug("Navigating to property URL: %s", property.link)
        page.goto(property.link)

        # Process the property
        self.logger.debug("Processing property ID: %s", property.id)
        try:
            # Find the hidden element that contains our image URLs
            image_element = page.locator("#HstrImg")
            if not image_element.count():
                raise ValueError("Could not find image container (#HstrImg) on page")

            # Get the images from the value attribute (comma-separated URLs)
            image_urls_string = image_element.get_attribute("value")
            if not image_urls_string:
                raise ValueError("Image element found but contains no URLs")

            # Split the comma-separated URLs into a list
            img_urls = image_urls_string.split(",")
            if not img_urls:
                raise ValueError("No image URLs found after splitting")

            self.logger.debug(
                "Successfully extracted %d image URLs from property page", len(img_urls)
            )

        except Exception as e:
            self.logger.error("Error retrieving image URLs: %s", str(e))
            img_urls = []

        # Get property details
        try:
            details_elements = page.locator("div.iconoDatos + p").all()
            details = [element.text_content() for element in details_elements]

            property.type = (
                self.possible_types.get(details[0].lower(), PropertyType.OTHER)
                if details and details[0] is not None
                else PropertyType.OTHER
            )

            property.details = PropertyDetails(
                operation=PropertyOperation(details[1]),
                neighborhood=details[2],
                n_rooms=int(re.sub(r"[^\d]", "", details[3])) if details[3] else None,
                n_bathrooms=(
                    int(re.sub(r"[^\d]", "", details[4])) if details[4] else None
                ),
                square_meters=(
                    float(re.sub(r"[^\d.]", "", details[5])) if details[5] else None
                ),
            )
        except Exception as e:
            self.logger.error("Error retrieving property details: %s", str(e))
            return None

        # Create a directory for the property's images
        property_img_dir = os.path.join(self.images_dir, property.id)
        os.makedirs(property_img_dir, exist_ok=True)

        jsonlines_data = []

        # Download and save images locally
        for i, img_url in enumerate(img_urls, 1):
            if any(img_url.endswith(ext) for ext in VALID_IMAGE_EXTENSIONS):

                # Get the image filename
                img_filename = img_url.split("/")[-1]
                img_path = os.path.join(property_img_dir, img_filename)
                self.logger.debug(
                    f"Downloading image {i}/{len(img_urls)} for property {property.id}: {img_filename}"
                )

                # Download and save the image
                try:
                    img_data = requests.get(img_url).content
                    with open(img_path, "wb") as f:
                        f.write(img_data)
                except Exception as e:
                    self.logger.error(f"Failed to download image {img_url}: {str(e)}")
                    continue

                # Prepare data for saving
                image_info = {
                    "source": "gallito",
                    "id": property.id,
                    "link": property.link,
                    "type": property.type.value,
                    "local_image_path": img_path,
                    "image_url": img_url,
                    "details": (
                        {
                            "operation": property.details.operation.value,
                            "neighborhood": property.details.neighborhood,
                            "n_rooms": property.details.n_rooms,
                            "n_bathrooms": property.details.n_bathrooms,
                            "square_meters": property.details.square_meters,
                        }
                        if property.details
                        else None
                    ),
                }
                jsonlines_data.append(image_info)

        self.logger.debug("Saving property data to JSONL")
        self.save_to_jsonl(property, jsonlines_data)

    def save_to_jsonl(self, property: Property, jsonlines_data: List[Dict]):
        """
        Save the property data to a JSONL file

        Args:
            property (Property): The property to save
            jsonlines_data (List[Dict]): The data to save
        """
        # Create the JSONL file path
        jsonl_path = os.path.join(self.properties_dir, f"{property.id}.jsonl")
        self.logger.debug("Saving data for property %s to %s", property.id, jsonl_path)

        try:
            with open(jsonl_path, "a", encoding="utf-8") as f:
                for item in jsonlines_data:
                    f.write(json.dumps(item, ensure_ascii=False) + "\n")
            self.logger.debug("Successfully saved data for property %s", property.id)
        except Exception as e:
            self.logger.error(
                "Failed to save data for property %s: %s", property.id, str(e)
            )
