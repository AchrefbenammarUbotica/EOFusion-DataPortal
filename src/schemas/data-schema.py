from sqlmodel import SQLModel, Field, Relationship, JSON, Column
from typing import Optional, List
from datetime import datetime

class AISData(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    mmsi: str = Field(default=None)
    timestamp: str = Field(default=None)
    latitude: float = Field(default=None)
    longitude: float = Field(default=None)
    cog: Optional[float] = Field(default=None)
    sog: Optional[float] = Field(default=None)
    heading: Optional[float] = Field(default=None)
    navstat: Optional[int] = Field(default=None)
    imo: Optional[int] = Field(default=None)
    name: Optional[str] = Field(default=None)
    callsign: Optional[str] = Field(default=None)
    vessel_type: Optional[int] = Field(default=None)
    a: Optional[int] = Field(default=None)
    b: Optional[int] = Field(default=None)
    c: Optional[int] = Field(default=None)
    d: Optional[int] = Field(default=None)
    draught: Optional[float] = Field(default=None)
    destination: Optional[str] = Field(default=None)
    eta: Optional[str] = Field(default=None)
    write_ts: str = Field(default_factory=datetime.utcnow)

class Vessel(SQLModel, table=True):
    imo: int = Field(primary_key=True)
    mmsi: str = Field(default=None)
    vessel_name: Optional[str] = Field(default=None)
    vessel_type: Optional[str] = Field(default=None)
    vesselfinder_url: Optional[str] = Field(default=None)
    flag: Optional[str] = Field(default=None)
    length: Optional[float] = Field(default=None)
    beam: Optional[float] = Field(default=None)
    year_built: Optional[int] = Field(default=None)
    statuses: List["VesselStatus"] = Relationship(back_populates="vessel")

class VesselStatus(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    imo: int = Field(foreign_key="vessel.imo")
    freshness: str
    latitude: float
    longitude: float
    speed: Optional[float] = Field(default=None)
    course: Optional[float] = Field(default=None)
    status: Optional[str] = Field(default=None)
    tonnage: Optional[float] = Field(default=None)
    draught: Optional[float] = Field(default=None)
    vessel: "Vessel" = Relationship(back_populates="statuses")
    passes: List["SatPass"] = Relationship(back_populates="status")

class SatPass(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    satellite: str
    timestamp: str
    latitude: float
    longitude: float
    image_url: Optional[str] = Field(default=None)

    status_id: int = Field(foreign_key="vesselstatus.id")
    status: "VesselStatus" = Relationship(back_populates="passes")

class TLE(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    satellite_id: str = Field(foreign_key="satellite.id")
    line1: str
    line2: str
    created_at: str = Field(default_factory=datetime.utcnow)

    satellite: "Satellite" = Relationship(back_populates="tles")

class Satellite(SQLModel, table=True):
    id: int = Field(primary_key=True)
    name: str
    orbit: List[float] = Field(sa_column=Column(JSON))

    tles: List["TLE"] = Relationship(back_populates="satellite")

    class Config:
        arbitrary_types_allowed = True
