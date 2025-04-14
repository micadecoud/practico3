from scrapers.cartelera import MoviesScraper
from settings.settings_cartelera import load_settings_cartelera
from settings.logger import custom_logger


logger = custom_logger("Main")


if __name__ == "__main__":

    # Set the settings
    settings = load_settings_cartelera(key="WebPage")
    BASE_URL = settings["BaseUrl"]
    AMOUNT_OF_MOVIES = settings["AmountOfMovies"]

    # Initialize the scraper
    scraper = MoviesScraper(
        max_movies=AMOUNT_OF_MOVIES,
    )

    # Run the scraper
    scraper.run(
        base_url=BASE_URL,
    )
