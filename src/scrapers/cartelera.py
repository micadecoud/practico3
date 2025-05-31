import json
import os
import io
import sys
from typing import Dict, List

sys.path.append(os.getcwd())

import requests
from playwright.sync_api import sync_playwright
from src.settings import custom_logger, load_settings_cartelera
from src.connectors.s3_client import S3Client
from src.structs import StorageType

VALID_IMAGE_EXTENSIONS = [".jpg", ".jpeg", ".png", ".webp"]

class MoviesScraper:
    def __init__(self, output_dir: str = "data/scraped_movies_data", max_movies: int = 60) -> None:
        self.logger = custom_logger(self.__class__.__name__)

        # Load storage config
        storage_config = load_settings_cartelera("Storage")
        self.storage_type = StorageType(storage_config["Type"])

        if self.storage_type == StorageType.S3:
            self.s3_client = S3Client(
                bucket_name=storage_config["S3"]["Bucket"],
                region_name=storage_config["S3"]["Region"]
            )

        # Paths
        self.output_dir = output_dir
        self.images_dir = os.path.join(self.output_dir, "images")
        self.movies_dir = os.path.join(self.output_dir, "movies")

        if self.storage_type == StorageType.LOCAL:
            os.makedirs(self.images_dir, exist_ok=True)
            os.makedirs(self.movies_dir, exist_ok=True)

        self.processed_movies = set()
        self._load_processed_movies()

        self.max_movies = max_movies
        self.logger.info("MoviesScraper initialized. Storage: %s", self.storage_type.value)

    def _load_processed_movies(self) -> None:
        if self.storage_type == StorageType.S3:
            try:
                response = self.s3_client.s3_client.list_objects_v2(
                    Bucket=self.s3_client.bucket_name, Prefix=f"{self.movies_dir}/"
                )
                if "Contents" in response:
                    for obj in response["Contents"]:
                        if obj["Key"].endswith(".jsonl"):
                            movie_id = os.path.basename(obj["Key"]).replace(".jsonl", "")
                            self.processed_movies.add(movie_id)
            except Exception as e:
                self.logger.error(f"Error loading movies from S3: {str(e)}")
        else:
            for filename in os.listdir(self.movies_dir):
                if filename.endswith(".jsonl"):
                    movie_id = filename.replace(".jsonl", "")
                    self.processed_movies.add(movie_id)

        self.logger.info(f"Found {len(self.processed_movies)} previously processed movies")

    def run(self, base_url: str) -> None:
        self.logger.info("Starting movie scraping...")

        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            page = browser.new_page()
            page.goto(base_url)

            article_elements = page.locator("article.evento")
            total_movies = article_elements.count()
            max_movies_to_get = min(self.max_movies, total_movies)

            for i in range(max_movies_to_get):
                article = article_elements.nth(i)
                titulo = article.locator("h2.name").text_content()

                event_data = article.locator("ul.event-data")
                datos = event_data.locator("li.text strong")

                genero = direccion = protagonistas = "N/A"
                for j in range(datos.count()):
                    texto = datos.nth(j).text_content().strip()
                    if j == 0:
                        genero = texto
                    elif j == 1:
                        direccion = texto
                    else:
                        protagonistas = texto

                poster_url = article.locator("div.poster-container a img").get_attribute("src")
                img_filename = poster_url.split("/")[-1]
                img_key = f"{self.images_dir}/{img_filename}"

                try:
                    img_data = requests.get(poster_url).content
                    if self.storage_type == StorageType.S3:
                        img_file = io.BytesIO(img_data)
                        content_type = "image/jpeg" if img_filename.endswith((".jpg", ".jpeg")) else "image/png"
                        success = self.s3_client.upload_image(img_file, key=img_key, content_type=content_type)
                        if not success:
                            raise Exception("Upload to S3 failed")
                        img_path = f"s3://{self.s3_client.bucket_name}/{img_key}"
                    else:
                        img_path = os.path.join(self.images_dir, img_filename)
                        with open(img_path, "wb") as f:
                            f.write(img_data)
                except Exception as e:
                    self.logger.error(f"Failed to save image: {e}")
                    continue

                image_info = {
                    "source": "cartelera",
                    "id": i,
                    "local_image_path": img_path,
                    "image_url": poster_url,
                    "details": {
                        "titulo": titulo,
                        "genero": genero,
                        "protagonistas": protagonistas,
                        "direccion": direccion,
                    },
                }

                self.save_to_jsonl(i, image_info)

            self.logger.info("Finished scraping movies.")
            browser.close()

    def save_to_jsonl(self, movie_id: int, data: Dict) -> None:
        if self.storage_type == StorageType.S3:
            json_key = f"{self.movies_dir}/{movie_id}.jsonl"
            try:
                success = self.s3_client.save_jsonl(data=[data], key=json_key)
                if not success:
                    raise Exception("Failed to upload JSONL to S3")
                self.logger.debug("Saved movie %s to S3", movie_id)
            except Exception as e:
                self.logger.error("Failed to save movie %s: %s", movie_id, str(e))
        else:
            jsonl_path = os.path.join(self.movies_dir, f"{movie_id}.jsonl")
            try:
                with open(jsonl_path, "w", encoding="utf-8") as f:
                    f.write(json.dumps(data, ensure_ascii=False) + "\n")
                self.logger.debug("Saved movie %s locally", movie_id)
            except Exception as e:
                self.logger.error("Error saving movie %s: %s", movie_id, str(e))
