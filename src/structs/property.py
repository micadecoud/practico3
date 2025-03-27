from enum import Enum
from typing import List, Optional

from pydantic import BaseModel


class PropertyType(Enum):
    APARTMENT = "apartamento"
    HOUSE = "casa"
    OTHER = "otro"
    UNKNOWN = "desconocido"


class PropertyOperation(Enum):
    SALE = "Venta"
    RENT = "Alquiler"


class PropertyDetails(BaseModel):
    operation: Optional[PropertyOperation]
    neighborhood: Optional[str]
    n_rooms: Optional[int]
    n_bathrooms: Optional[int]
    square_meters: Optional[float]


class Property(BaseModel):
    id: str
    link: str
    type: PropertyType
    images: List[str]
    details: Optional[PropertyDetails] = None
