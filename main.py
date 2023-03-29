from fastapi import FastAPI
from pydantic import BaseModel
from typing import List
from geopy.distance import geodesic
import databases
import sqlalchemy

# create the FastAPI app
app = FastAPI()

# create the database connection
DATABASE_URL = "sqlite:///./addresses.db"
database = databases.Database(DATABASE_URL)
metadata = sqlalchemy.MetaData()

# define the addresses table schema
addresses = sqlalchemy.Table(
    "addresses",
    metadata,
    sqlalchemy.Column("id", sqlalchemy.Integer, primary_key=True),
    sqlalchemy.Column("name", sqlalchemy.String),
    sqlalchemy.Column("address", sqlalchemy.String),
    sqlalchemy.Column("latitude", sqlalchemy.Float),
    sqlalchemy.Column("longitude", sqlalchemy.Float),
)

# create the addresses table in the database
engine = sqlalchemy.create_engine(
    DATABASE_URL, connect_args={"check_same_thread": False}
)
metadata.create_all(engine)


# define the Address schema
class Address(BaseModel):
    name: str
    address: str
    latitude: float
    longitude: float

    class Config:
        orm_mode = True


# create a new address
@app.post("/addresses/")
async def create_address(address: Address):
    query = addresses.insert().values(
        name=address.name,
        address=address.address,
        latitude=address.latitude,
        longitude=address.longitude,
    )
    address_id = await database.execute(query)
    return {**address.dict(), "id": address_id}


# update an existing address
@app.put("/addresses/{address_id}")
async def update_address(address_id: int, address: Address):
    query = (
        addresses.update()
        .where(addresses.c.id == address_id)
        .values(
            name=address.name,
            address=address.address,
            latitude=address.latitude,
            longitude=address.longitude,
        )
    )
    await database.execute(query)
    return {**address.dict(), "id": address_id}


# delete an existing address
@app.delete("/addresses/{address_id}")
async def delete_address(address_id: int):
    query = addresses.delete().where(addresses.c.id == address_id)
    await database.execute(query)
    return {"message": "Address deleted successfully"}


# retrieve all addresses
@app.get("/addresses/")
async def get_addresses():
    query = addresses.select()
    return await database.fetch_all(query)


# retrieve addresses within a given distance and location coordinates
@app.get("/addresses/nearby/")
async def get_nearby_addresses(latitude: float, longitude: float, distance: float):
    query = addresses.select()
    addresses_list = await database.fetch_all(query)
    nearby_addresses = []
    for address in addresses_list:
        if geodesic((latitude, longitude), (address["latitude"], address["longitude"])).km <= distance:
            nearby_addresses.append(address)
    return nearby_addresses


# connect to the database before starting the server
@app.on_event("startup")
async def startup():
    await database.connect()


# disconnect from the database after stopping the server
@app.on_event("shutdown")
async def shutdown():
    await database.disconnect()
