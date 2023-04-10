import sqlalchemy
from datetime import datetime
import databases
from pydantic import BaseModel
from fastapi import FastAPI, UploadFile, File
from fastapi.middleware.cors import CORSMiddleware
from typing import List, Union
import urllib
import os
from sqlalchemy.orm import sessionmaker
from azure.storage.blob.aio import BlobServiceClient
from azure.storage.blob import ContainerClient
import algorithms.pyprefixspan as prefixspanalgorithm
from prefixspan import PrefixSpan
import algorithms.clofast as cs
import algorithms.pfpm as pfpm
import time

"""host_server = os.environ.get('host_server', 'localhost')
db_server_port = urllib.parse.quote_plus(
    str(os.environ.get('db_server_port', '5432')))
database_name = os.environ.get('database_name', 'isguvenligikds')
db_username = urllib.parse.quote_plus(
    str(os.environ.get('db_username', 'isguvenligikds')))
db_password = urllib.parse.quote_plus(
    str(os.environ.get('db_password', 'halil_012')))"""

"""SQL bağlantısı için gerekli url değişkeni"""
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

algs = sqlalchemy.Table(
    'algorithms',
    metadata,
    sqlalchemy.Column("id", sqlalchemy.Integer, primary_key=True),
    sqlalchemy.Column("algorithm_name", sqlalchemy.String),
    sqlalchemy.Column("file_name", sqlalchemy.String),
    sqlalchemy.Column("created_on", sqlalchemy.String)
)

codes = sqlalchemy.Table(
    'kazakodları',
    metadata,
    sqlalchemy.Column("code", sqlalchemy.Integer, primary_key=True),
    sqlalchemy.Column("accident", sqlalchemy.String)
)

engine = sqlalchemy.create_engine(DATABASE_URL)
Session = sessionmaker(bind=engine)
session = Session()

metadata.create_all(engine)
print("sql connection created")


class AlgorithmData(BaseModel):
    id: int
    algorithm_type: int
    file_name: str
    created_on: str


"""class AlgorithmDataRequest(BaseModel):
    algorithm_type: int
    file_name: str
    data_file: UploadFile"""

"""Api bağlantısı sağlamak için FastApi Framework'ü kullanıldı"""
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

"""
Verilerin bulut üzerinde tutulması için Azure Blob Service kullanıldı. Bu servis bir verinin depolanmasını ve çağrılmasını sağlamakta.
Bu API verinin yüklenmesi için kullanılıyor.
"""


async def uploadtoazure(file: UploadFile, file_name: str):
    connect_str = "DefaultEndpointsProtocol=https;AccountName=ytukdsprojectdata;AccountKey=E/5tHAZVSYzzvkvIHa4IInz+38dMKJulcNFj9eeIHN9G1XI9MN3eZxam4lwlkgU1OI7j+hxAEqXK+AStPQj36Q==;EndpointSuffix=core.windows.net"
    blob_service_client = BlobServiceClient.from_connection_string(connect_str)
    container_name = "algorithmdatas"
    async with blob_service_client:
        container_client = blob_service_client.get_container_client(
            container_name)
        try:
            blob_client = container_client.get_blob_client(file_name)
            f = file.read()
            await blob_client.upload_blob(f)
        except Exception as e:
            print(e)
            return HTTPException(401, "Something went terribly wrong..")
    return

""".
Bu API Blob Service'e yüklenmiş bir verinin indirilmesi için kullanılıyor.
"""


async def download_blob(filename: str):
    connect_str = "DefaultEndpointsProtocol=https;AccountName=ytukdsprojectdata;AccountKey=E/5tHAZVSYzzvkvIHa4IInz+38dMKJulcNFj9eeIHN9G1XI9MN3eZxam4lwlkgU1OI7j+hxAEqXK+AStPQj36Q==;EndpointSuffix=core.windows.net"
    blob_service_client_instance = BlobServiceClient.from_connection_string(
        conn_str=connect_str)

    blob_client_instance = blob_service_client_instance.get_blob_client(
        container="algorithmdatas", blob=filename, snapshot=None)

    # with open("exp.txt", "wb") as my_blob:
    blob_data = await blob_client_instance.download_blob()
    my_str = await blob_data.content_as_text()
    return my_str

"""
Bu API aracılığı ile kullanılcak bir veri ilgili tabloya kaydediliyor.
Verinin hangi algoritma ile kullanılacağı, hangi tarihte yüklendiği kaydediliyor.
"""


@app.post("/add_algorithm_data/")
async def Create_algdata(algorithm_type: int, file: UploadFile = File(...)):
    d = datetime.now()
    nameoffile = "Algorithm_file_" + str(algorithm_type) + "_" + \
        str(d.month) + str(d.day) + str(d.hour) + \
        str(d.minute) + str(d.second) + ".txt"
    print((nameoffile))
    try:
        await uploadtoazure(file.file, nameoffile)
    except Exception as e:
        return "Dosyayı yüklerken bir problem yaşandı: " + e

    query = alg_datas.insert().values(algorithm_type=algorithm_type,
                                      file_name=nameoffile, created_on=str(d))
    last_record_id = await database.execute(query)
    print(last_record_id)
    return

"""
Yüklenmiş bütün verileri çağıran API
"""


@app.get("/get_all_datas/")
def Create_algdata():
    query = sqlalchemy.select(alg_datas)
    print((query))
    with engine.connect() as session:
        for row in session.execute(query):
            yield row


"""
Girilen id'nin verisini getiryor
"""


@app.get("/get_data/")
async def Create_algdata(data_id: int):
    d = session.query(alg_datas).filter_by(id=data_id).first()
    f = await download_blob(d.file_name)
    return f

"""
Prefixspan_agp algoritmasını çalıştıran API
data_id: çalışacak verinin id'si
minsup: Algoritma için belirlenen minimum support değeri
length: Algoritma için belirlenen length değeri
"""


@app.get("/prefixspan_agp/")
async def PrefixspanAlgorithmAPI(data_id: int, minsup: float, length: int = 1):
    d = session.query(alg_datas).filter_by(id=data_id).first()
    s = await download_blob(d.file_name)
    s = s.strip()
    s = s.replace(' ', '')
    s = ''.join(s.split())
    s = s.replace('-1', ' ')
    d1 = s.split(' -2')
    d1.pop()
    d2 = []
    for i in d1:
        d2.append(i.split(' '))
    ps = PrefixSpan(d2)
    if (minsup < 1):
        min_sup = int(len(d2) * minsup)
    result = ps.frequent(minsup)
    return (str(e[1]) + ' : ' + str(e[0]) for e in result)

"""
Clofast algoritmasını çalıştıran API
data_id: çalışacak verinin id'si
minsup: Algoritma için belirlenen minimum support değeri
"""


@app.get("/clofast/")
async def ClofastAPI(data_id: int, minsup: float):
    d = session.query(alg_datas).filter_by(id=data_id).first()
    s = await download_blob(d.file_name)
    data = cs.prepare_clofast_data(s)
    cf = cs.Clofast(data, minsup)
    cf.setminsup((minsup))
    cf.frequent_item_set_mining()
    result = cf.get_result()
    return result

"""
PFPM algoritmasını çalıştıran API
data_id: çalışacak verinin id'si
minsup: Algoritma için belirlenen minimum support değeri
length: Algoritma için belirlenen maximum perception değeri
"""


@app.get("/pfpm/")
async def PfpmAPI(data_id: int, minsup: float, maxPer: float = 800):
    d = session.query(alg_datas).filter_by(id=data_id).first()
    raw_data = await download_blob(d.file_name)
    s = pfpm.prepare_pfpm_data(raw_data)
    ap = pfpm.PFPMC(s, minsup, maxPer, sep=' ')
    ap.startMine()
    return ap.getPatternsAsString()

"""
Verilen data için optimum bir minimum support değeri belirleyen API
Minimum support değerini adım adım küçülterek izin verilen süreye kadar algoritmayı çalıştırmaktadır.
"""


@app.get("/getOptimumMinSup")
async def GetMinSup(data_id: int, algorithm_type: int, maxSecond: int = 120):
    t = 0
    minsup = 0.9
    while (maxSecond > t):
        st = time.time()
        match algorithm_type:
            case 0:
                PrefixspanAlgorithmAPI(data_id, minsup)
            case 1:
                ClofastAPI(data_id, minsup)
            case 2:
                PfpmAPI(data_id, minsup)
        et = time.time()
        t = et - st
        minsup *= 0.9
