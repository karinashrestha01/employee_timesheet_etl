# db/db_utils.py

import sys, os, logging
from dotenv import load_dotenv
from pathlib import Path
from sqlalchemy import create_engine
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import sessionmaker, Session
import pandas as pd
import numpy as np

sys.path.append(str(Path(__file__).resolve().parent.parent))
from db.models import Base

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)
load_dotenv()

DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "8432")
DB_NAME = os.getenv("DB_NAME")

if not all([DB_USER, DB_PASSWORD, DB_NAME]):
    raise EnvironmentError("Missing one or more database environment variables.")

def _build_connection_url() -> str:
    return f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

ENGINE = create_engine(_build_connection_url(), echo=False, pool_pre_ping=True)
SessionLocal = sessionmaker(bind=ENGINE)

def get_engine():
    """Get the database engine instance."""
    return ENGINE


def create_all_tables() -> None:
    try:
        Base.metadata.create_all(ENGINE)
        logger.info("Tables created successfully")
    except SQLAlchemyError as exc:
        logger.exception("Failed to create tables")
        raise exc

def clean_df_for_sql(df: pd.DataFrame) -> pd.DataFrame:
    df = df.replace({pd.NaT: None, pd.NA: None, np.nan: None})
    return df.where(pd.notna(df), None)

def upsert_dataframe(df: pd.DataFrame, table_class, session: Session, key_cols: list, batch_size: int = 500):
    """Perform SCD2/UPSERT for the columns"""
    df = clean_df_for_sql(df)
    records = df.to_dict(orient="records")
    
    for i in range(0, len(records), batch_size):
        batch = records[i:i+batch_size]
        stmt = insert(table_class).values(batch)
        # ON CONFLICT → update only end_date for historical SCD2
        conflict_cols = {col.name: col for col in table_class.__table__.columns if col.name not in key_cols}
        stmt = stmt.on_conflict_do_update(
            index_elements=key_cols,
            set_={col: stmt.excluded[col] for col in conflict_cols}
        )
        session.execute(stmt)
        session.commit()
        logger.info(f"Upserted batch {i // batch_size + 1} ({len(batch)} records)")

def get_session() -> Session:
    return SessionLocal()
