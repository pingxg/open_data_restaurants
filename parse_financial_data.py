
from sqlalchemy import create_engine, Column, Integer, String, Float, select
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from geopy.geocoders import Nominatim
from geopy.extra.rate_limiter import RateLimiter
from bs4 import BeautifulSoup
from pprint import pprint
import time
import pandas as pd
from DrissionPage import ChromiumPage, SessionPage

# 用 d 模式创建页面对象（默认模式）
# page = ChromiumPage()

import streamlit as st

db_user = st.secrets["db_user"]
db_password = st.secrets["db_password"]
db_host = st.secrets["db_host"]
db_name = st.secrets["db_name"]
db_port = st.secrets["db_port"]

db_connection_string = f"mysql+pymysql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"

# Create engine and session
engine = create_engine(db_connection_string)
# Session = sessionmaker(bind=engine)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

# session = Session()

Base = declarative_base()

class Company(Base):
    __tablename__ = 'company'
    id = Column(Integer, primary_key=True)
    name = Column(String(45))
    business_id = Column(String(45))


def process_companies(company_ids):
    """Process a list of company names to find their business IDs."""
    page = ChromiumPage()  # Each thread gets its own page object.
    session = SessionLocal()  # Create a new session for this thread.

    for i in company_ids:
        i_cleaned = i.replace('-', '')
        try:
            page.get(f'https://www.asiakastieto.fi/yritykset/fi/{i_cleaned}/taloustiedot')
            # page.ele('xpath://html/body/div[4]/main/div[1]/header/div/div/div[1]/div[1]/div/div/header/div/div/form/input').input(i)
            try:
                no_data_info = page.ele('text=Taloustietoja ei ole saatavilla')
                if no_data_info:
                    print(f'No data for {i}')
                    continue
            except:
                pass

            soup = BeautifulSoup(page.html, 'html.parser')
            table_data = []
            # Find the first table
            table = soup.find('table')

            hdrs = table.find_all('th')
            table_data.append([i.text.split("/")[-1].strip() for i in hdrs])
            # Extracting all rows from the table
            rows = table.find_all('tr')

            # Loop through rows to extract data
            for row in rows:
                # Find all data for each row
                columns = row.find_all('td')
                data = [col.text.replace(" ", "").strip() for col in columns]
                if "Liikevaihto(1000€)" in data or "Liikevoitto(-tappio)(1000€)" in data:
                    table_data.append(data)
            df_transposed = pd.DataFrame(table_data).transpose().reset_index()
            df_transposed.columns = ['business_id','year', 'revenue', 'profit']
            df_numeric = df_transposed.apply(pd.to_numeric, errors='coerce')

            # Drop rows with NaN values that resulted from the coercion
            df_cleaned = df_numeric.dropna()
            company = session.query(Company).filter(Company.business_id == i).first()
            df_cleaned['business_id'] = company.id
            df_cleaned = df_cleaned.astype(int)
            df_cleaned.rename(columns={'business_id': 'company_id'}, inplace=True)

            print(df_cleaned)
            df_cleaned.to_sql('financial_data', engine, if_exists='append', index=False)  # Replace 'financial_table_name' with your actual table name




        except Exception as e:
            print(f"Error processing {i}: {str(e)}")
    session.close()  # Close the session when done.
    page.quit()  # Close the browser when done.


from threading import Thread

def main(workers=2):
    # Assume this is your list of company names to process.
    session = SessionLocal()  # Create a new session for this thread.
    results = session.query(Company).all()
    company_ids = [i.business_id for i in results]
    # Divide the company names among the specified number of workers.
    names_per_worker = len(company_ids) // workers
    threads = []

    for i in range(workers):
        start_index = i * names_per_worker
        end_index = start_index + names_per_worker
        worker_names = company_ids[start_index:end_index]

        # Handle any remaining company names for the last worker.
        if i == workers - 1:
            worker_names += company_ids[end_index:]

        # Create and start a new thread for each worker.
        thread = Thread(target=process_companies, args=(worker_names,))
        threads.append(thread)
        thread.start()

    # Wait for all threads to complete.
    for thread in threads:
        thread.join()

if __name__ == '__main__':
    main(workers=1)  # Specify the number of workers here.

