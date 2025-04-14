import json
import os
import re
from typing import Dict, List
import sys

sys.path.append(os.getcwd())

from playwright.sync_api import Browser, Page, sync_playwright
import requests

from src.settings import custom_logger
#from src.structs import PropertyType, Property, PropertyDetails, PropertyOperation


VALID_IMAGE_EXTENSIONS = [".jpg", ".jpeg", ".png", ".webp"]


class MoviesScraper:
    def __init__(
        self, output_dir: str = "data/scraped_movies_data", max_movies: int = 60
    ) -> None:
        """
        Initialize the MoviesScraper

        Args:
            output_dir (str): The directory to store scraped data
            max_movies (int): The maximum number of movies to scrape
        """

        self.logger = custom_logger(self.__class__.__name__)

        # Set up directories for storing scraped data
        self.output_dir = output_dir
        self.images_dir = os.path.join(self.output_dir, "images")
        self.movies_dir = os.path.join(self.output_dir, "movies")

        # Create necessary directories if they don't exist
        os.makedirs(self.output_dir, exist_ok=True)
        os.makedirs(self.images_dir, exist_ok=True)
        os.makedirs(self.movies_dir, exist_ok=True)

        # Keep track of processed movies to avoid duplicates
        self.processed_movies = set()
        self._load_processed_movies()

        # Initialize counters for processed movies
        self.processed_movies = len(self.processed_movies)
        self.max_movies = max_movies
        self.logger.info(
            "MoviesScraper initialized. Output directory: %s", self.output_dir
        )

    def _load_processed_movies(self) -> None:
        """Load already processed movies from existing JSONL files"""

        for filename in os.listdir(self.movies_dir):
            if filename.endswith(".jsonl"):
                movie_id = filename.replace(".jsonl", "")
                self.processed_movies.add(movie_id)
        self.logger.info(
            f"Found {len(self.processed_movies)} previously processed movies"
        )

    def run(self, base_url: str) -> None:
        """
        Run the movies scraper.

        Args:
            base_url (str): The base URL for movies listings.
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

            # Construct the URL for the current page
            page_url = f"{base_url}"
            self.logger.info(f"Processing page {page_url}")

            # Navigate to the page
            page.goto(page_url)

            self.logger.info(page)

            # Esperar a que los artículos estén disponibles
            #page.wait_for_selector('article.evento')
    
            # Obtener todos los elementos de article.evento
            article_elements = page.locator('article.evento')
            
            # Obtener el número de peliculas disponibles
            total_movies = article_elements.count()

            # Limitar el número de peliculas a obtener
            max_movies_to_get = min(self.max_movies, total_movies)

            # Recorrer la cartelera hasta el máximo
            for i in range(max_movies_to_get):
                # Localizar cada pelicula (por índice)
                article = article_elements.nth(i)
                
                # Obtener el titulo de la pelicula
                titulo = article.locator('h2.name').text_content()

                # Obtener datos del evento
                event_data = article.locator('ul.event-data')

                datos = event_data.locator('li.text strong')
                
                genero = "N/A"
                direccion = "N/A"
                protagonistas = "N/A"

                for j in range(datos.count()):
                    texto = datos.nth(j).text_content().strip()

                    if j==0:
                        genero=texto 
                    elif j==1:
                        direccion=texto
                    else:
                        protagonistas=texto
    
                # Obtener poster del evento
                poster = article.locator('div.poster-container a img').get_attribute('src')

                # Get the image filename
                img_filename = poster.split("/")[-1]
                img_path = os.path.join(self.images_dir, img_filename)
                

                # Descargar y guardar la imagen
                try:
                    img_data = requests.get(poster).content
                    with open(img_path, "wb") as f:
                        f.write(img_data)
                except Exception as e:
                    self.logger.error(f"Failed to download image {poster}: {str(e)}")
                    continue

                
                # Imprimir el contenido del artículo
                print(f"Artículo {i + 1}:\ntitulo: {titulo}\ngenero: {genero}\ndireccion: {direccion}\nprotagonistas: {protagonistas}\nposter: {poster}\n")
                
                # Prepare data for saving
                image_info = {
                    "source": "cartelera",
                    "id": i,
                    "local_image_path": img_path,
                    "image_url": poster,
                    "details": (
                        {
                            "titulo": titulo,
                            "genero": genero,
                            "protagonistas": protagonistas,
                            "direccion": direccion,
                        }
                    ),
                }

                self.logger.debug("Saving movies data to JSONL")
                jsonl_path = os.path.join(self.movies_dir, f"{i}.jsonl")
                self.logger.debug("Saving data for movie %s to %s", i, jsonl_path)

                try:
                    with open(jsonl_path, "a", encoding="utf-8") as f:
                        f.write(json.dumps(image_info, ensure_ascii=False) + "\n")
                    self.logger.debug("Successfully saved data for movie %s", i)
                except Exception as e:
                    self.logger.error(
                        "Failed to save data for movie %s: %s", i, str(e)
                    )
            
            self.logger.info("Finished processing all movies")
            browser.close()



    