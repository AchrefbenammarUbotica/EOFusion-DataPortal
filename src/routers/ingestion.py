from fastapi import APIRouter, Depends
from sqlmodel import Session
from src.services.db import get_session
from src.schemas.data_schema import Vessel, Satellite

router = APIRouter()

@router.post("/vessels/")
def ingest_vessel(vessel: Vessel, session: Session = Depends(get_session)):
    session.add(vessel)
    session.commit()
    session.refresh(vessel)
    return vessel

@router.post("/satellites/")
def ingest_satellite(satellite: Satellite, session: Session = Depends(get_session)):
    session.add(satellite)
    session.commit()
    session.refresh(satellite)
    return satellite
