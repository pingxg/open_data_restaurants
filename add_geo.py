from sqlalchemy import create_engine, Column, Integer, String, Float, select
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from geopy.geocoders import Nominatim
from geopy.extra.rate_limiter import RateLimiter

import time
import streamlit as st

db_user = st.secrets["db_user"]
db_password = st.secrets["db_password"]
db_host = st.secrets["db_host"]
db_name = st.secrets["db_name"]
db_port = st.secrets["db_port"]

db_connection_string = f"mysql+pymysql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"

# Create engine and session
engine = create_engine(db_connection_string)
Session = sessionmaker(bind=engine)
session = Session()

Base = declarative_base()

class Restaurant(Base):
    __tablename__ = 'restaurants'
    id = Column(Integer, primary_key=True)
    name = Column(String(45))
    type = Column(String(45))
    address = Column(String(45))
    post = Column(String(45))
    city = Column(String(45))
    latitude = Column(Float)
    longitude = Column(Float)
    company_name = Column(String(45))
    company_id = Column(String(45))

# Initialize the geocoder
geolocator = Nominatim(user_agent="MyAppGeocoder")
# Use rate limiter to avoid overloading the geocoding service
geocode = RateLimiter(geolocator.geocode, min_delay_seconds=1)

def update_geocode():
    # Fetch all restaurants
    restaurants = session.query(Restaurant).all()
    
    for restaurant in restaurants:
        if not restaurant.latitude or not restaurant.longitude:  # Check if geocode is missing
            full_address = f"{restaurant.address}, {restaurant.post} {restaurant.city}"
            location = geocode(full_address)
            if location:
                restaurant.latitude = location.latitude
                restaurant.longitude = location.longitude
                print(f"Updated {restaurant.name} with lat: {location.latitude}, long: {location.longitude}")
                session.commit()
            else:
                print(f"Could not geocode {restaurant.name}")
            time.sleep(1)  # Respect the service's rate limit

if __name__ == "__main__":
    update_geocode()
    session.close()