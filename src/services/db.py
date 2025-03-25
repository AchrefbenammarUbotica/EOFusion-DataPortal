from sqlmodel import SQLModel, create_engine, Session
from src.config.settings import get_settings

settings = get_settings()

engine = create_engine(settings.DATABASE_URL, echo=settings.DEBUG)

def get_session():
    with Session(engine) as session:
        yield session


def init_db():
    SQLModel.metadata.create_all(engine)