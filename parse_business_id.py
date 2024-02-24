from sqlalchemy import create_engine, Column, Integer, String, Float, select
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from geopy.geocoders import Nominatim
from geopy.extra.rate_limiter import RateLimiter

import time

from DrissionPage import ChromiumPage

# 用 d 模式创建页面对象（默认模式）
page = ChromiumPage()

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


def process_companies(company_names):
    """Process a list of company names to find their business IDs."""
    page = ChromiumPage()  # Each thread gets its own page object.
    session = SessionLocal()  # Create a new session for this thread.

    for i in company_names:
        try:
            page.get('https://www.asiakastieto.fi/yritykset/haku?type=BUSINESS')
            page.ele('xpath://html/body/div[4]/main/div[1]/header/div/div/div[1]/div[1]/div/div/header/div/div/form/input').input(i)
            page.ele('xpath://html/body/div[4]/main/div[1]/header/div/div/div[1]/div[1]/div/div/header/div/div/form/button').click()
            names = page.eles('.td search-result__name ng-binding')
            ids = page.eles('.td search-result__business-id ng-binding')
            for name, id in zip(names, ids):
                _name = name.text.strip()
                _id = id.text.replace(' ', '').replace('Lakannut', '').strip()
                if i.lower() == _name.lower():
                    print(_name, _id)
                    # Update the company in the database with the found business ID.
                    company = session.query(Company).filter(Company.name == i).first()
                    if company:
                        company.business_id = _id
                        session.commit()

        except Exception as e:
            print(f"Error processing {i}: {str(e)}")
    session.close()  # Close the session when done.
    page.quit()  # Close the browser when done.


from threading import Thread

def main(workers=2):
    # Assume this is your list of company names to process.
    session = SessionLocal()  # Create a new session for this thread.
    results = session.query(Company).filter(Company.business_id == '1').all()
    company_names = [i.name for i in results]
    print(company_names)
    # Divide the company names among the specified number of workers.
    names_per_worker = len(company_names) // workers
    threads = []

    for i in range(workers):
        start_index = i * names_per_worker
        end_index = start_index + names_per_worker
        worker_names = company_names[start_index:end_index]

        # Handle any remaining company names for the last worker.
        if i == workers - 1:
            worker_names += company_names[end_index:]

        # Create and start a new thread for each worker.
        thread = Thread(target=process_companies, args=(worker_names,))
        threads.append(thread)
        thread.start()

    # Wait for all threads to complete.
    for thread in threads:
        thread.join()

if __name__ == '__main__':
    main(workers=1)  # Specify the number of workers here.

