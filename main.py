import sqlalchemy
from datetime import datetime
import databases
from pydantic import BaseModel
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from typing import List
import urllib
import os

"""host_server = os.environ.get('host_server', 'localhost')
db_server_port = urllib.parse.quote_plus(
    str(os.environ.get('db_server_port', '5432')))
database_name = os.environ.get('database_name', 'isguvenligikds')
db_username = urllib.parse.quote_plus(
    str(os.environ.get('db_username', 'isguvenligikds')))
db_password = urllib.parse.quote_plus(
    str(os.environ.get('db_password', 'halil_012')))"""
DATABASE_URL = 'postgresql://isguvenligikds:halil_012@isguvenligikds.postgres.database.azure.com/isguvenligikds?sslmode=require'


metadata = sqlalchemy.MetaData()

alg_datas = sqlalchemy.Table(
    'datas',
    metadata,
    sqlalchemy.Column("id", sqlalchemy.Integer, primary_key=True),
    sqlalchemy.Column("algorithm_type", sqlalchemy.Integer),
    sqlalchemy.Column("file_name", sqlalchemy.String),
    sqlalchemy.Column("created_on", sqlalchemy.String)
)

engine = sqlalchemy.create_engine(DATABASE_URL)

metadata.create_all(engine)


class AlgorithmData(BaseModel):
    id: int
    algorithm_type: int
    file_name: str
    created_on: str


class AlgorithmDataRequest(BaseModel):
    algorithm_type: int
    file_name: str


app = FastAPI(title="İş güvenliği için karar destek sistemi")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"]
)


database = databases.Database(DATABASE_URL)


@app.on_event("startup")
async def startup():
    await database.connect()


@app.on_event("shutdown")
async def shutdown():
    await database.disconnect()


@app.post("/add_algorithm/")
async def Create_algdata(data: AlgorithmDataRequest):
    query = alg_datas.insert().values(algorithm_type=data.algorithm_type,
                                      file_name=data.file_name, created_on=str(datetime.now()))
    last_record_id = await database.execute(query)
    print(last_record_id)
    return
