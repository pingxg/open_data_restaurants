import re

from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
import re

import streamlit as st

db_user = st.secrets["db_user"]
db_password = st.secrets["db_password"]
db_host = st.secrets["db_host"]
db_name = st.secrets["db_name"]
db_port = st.secrets["db_port"]

db_connection_string = f"mysql+pymysql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"

# Create engine
engine = create_engine(db_connection_string, echo=False)  # Set echo to False to reduce output

# Define the sessionmaker
Session = sessionmaker(bind=engine)

# Define the path to the file
file_path = 'oiva_helsinki_espoo_vanta.txt'


# Define the pattern to match the required information
pattern = re.compile(
    r"^(.*?)\n"  # Restaurant name
    r"(.+?)\n"  # Company name
    r'(?P<type>.+?)\n'  # Type
    r"([^\n]+)\n"  # Address
    r"(\d{5})\s"  # Post code
    r"(HELSINKI|ESPOO|VANTAA)$",  # City name
    re.MULTILINE
)

# Path to your file
# Initialize a list to hold parsed data
parsed_data = []


# Function to insert data
def insert_data(file_path):
    session = Session()
    with open(file_path, 'r', encoding='utf-8') as file:
        content = file.read()
        matches = pattern.findall(content)
        
        for match in matches:
            restaurant_name, company_name, type, address, post_code, city_name = match
            # Direct SQL insert statement using text construct for parameterized queries
            insert_stmt = text("""
                INSERT INTO financial.restaurants (name, company_name, type, address, post, city) 
                VALUES (:name, :company_name, :type, :address, :post, :city)
            """)
            session.execute(insert_stmt, {
                'name': restaurant_name.strip(),
                'company_name': company_name.strip(),
                'type': type.strip().lower(),
                'address': address.strip(),
                'post': post_code.strip(),
                'city': city_name.strip().lower()
            })
        
        # Commit the session to the database
        session.commit()

        # Close the session
        session.close()




