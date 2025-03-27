from typing import Any

from yaml import load
from yaml.loader import SafeLoader


def load_settings(key: str | None = None) -> dict[str, Any]:
    """
    Loads the settings from the config.yml file.

    Args:
        key (str | None): Optional key to get a specific value.

    Returns:
        dict[str, Any]: The settings.
    """

    with open("src/settings/config.yml", "r") as f:
        config = load(f, Loader=SafeLoader)

    if key:
        return config[key]

    return config
