import os
import json
from functools import lru_cache
from pydantic_settings import BaseSettings
from pydantic import ConfigDict

from typing import Literal

class Settings(BaseSettings):
    """
    Settings class for this application.
    Utilizes the BaseSettings from pydantic for environment variables.
    """
    # Environment
    ENVIRONMENT: Literal["local", "dev", "prod"] = "local"
    DATABASE_URL: str = "sqlite:///./Data.db"
    DEBUG: bool = False
    
    
    # Copernicus Settings
    CATALOGUE_URL: str 
    AUTH_URL: str 
    COLLECTION_NAME: str
    PRODUCT_TYPE: str 
    USERNAME: str 
    PASSWORD: str 
    
    
    # AISHub Settings
    AISHUB_URL: str 
    
    
    # N2YO Settings
    N2YO_API_KEY: str 
    model_config = ConfigDict(extra="ignore" , env_file = ".env")
        
    


@lru_cache(maxsize=None)
def get_settings() -> Settings:
    """Function to get and cache settings.
    The settings are cached to avoid repeated disk I/O."""
    environment = os.getenv("ENVIRONMENT", "local")
    
    if environment == "local":
        settings_file = ".env"
    elif environment == "dev":
        settings_file = ".env.dev"
    elif environment == "prod":
        settings_file = ".env.prod"
    else:
        raise ValueError(f"Invalid environment: {environment}")

    return Settings(_env_file=settings_file)  