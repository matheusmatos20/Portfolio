from fastapi import FastAPI, HTTPException, Depends
from sqlalchemy import create_engine, Column, Integer, String, Float
from sqlalchemy.orm import sessionmaker, Session, declarative_base
from pydantic import BaseModel
from typing import List
import pandas as pd
from dotenv import load_dotenv
import os


load_dotenv()

POSTGRES_USER = 'admin'
POSTGRES_PSW = 'admin123'
POSTGRES_HOST = 'localhost'
POSTGRES_DB = 'mydb'

SQLALCHEMY_DATABASE_URL = f"postgresql://{POSTGRES_USER}:{POSTGRES_PSW}@{POSTGRES_HOST}:5432/{POSTGRES_DB}"

engine = create_engine(SQLALCHEMY_DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

Base = declarative_base()

class Media(Base):
    __tablename__ = 'medias_linhas_veiculos'
    
    linha = Column(Integer, primary_key=True, index=True)
    media_diferenca = Column(Float, index=True)
    media_minutos = Column(String, index=True)

class TabelaResponse(BaseModel):
    linha: int
    media_diferenca: float
    media_minutos: str

    class Config:
        from_attributes = True

app = FastAPI()


@app.get("/medias_linhas_veiculos/", response_model=List[TabelaResponse])
def read_media():
    media_df = pd.read_sql('SELECT * FROM public.medias_linhas_veiculos', engine)

    media_list = media_df.to_dict(orient='records')
    return [TabelaResponse(**media) for media in media_list]

@app.get("/linha/{linha}", response_model=List[TabelaResponse])
def read_media(linha: int):
    media_df = pd.read_sql(f'SELECT * FROM public.medias_linhas_veiculos WHERE linha = {linha}', engine)

    if media_df.empty:
        raise HTTPException(status_code=404, detail="Linha não encontrado")
    
    media_list = media_df.to_dict(orient='records')
    return [TabelaResponse(**media) for media in media_list]


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("api:app", host="127.0.0.1", port=8000, reload=True)
