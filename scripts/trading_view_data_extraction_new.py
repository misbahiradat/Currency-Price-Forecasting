# Author @ Misbah Iradat
# Import necessary libraries
from tvDatafeed import TvDatafeed, Interval
from sqlalchemy import create_engine, Column, Integer, Date, Float, func
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from datetime import datetime
import logging
from datetime import datetime, date 
from pandas._libs.tslibs.timestamps import Timestamp 

# Configure logging for the program
logging.basicConfig(filename='tradingview_data_extraction.log', level=logging.DEBUG)
logging.info("Program started at {}".format(datetime.now()))

# Define SQLAlchemy Base
Base = declarative_base()



# Function to create the SQLAlchemy engine and session
def get_engine_from_settings():
    engine = create_engine('postgresql://postgres:postgres@localhost:5432/currencydb')
    return engine

def get_session():
    engine = get_engine_from_settings()
    Session = sessionmaker(bind=engine)
    session = Session()
    return session

# Function to check if a table exists in the database
def table_exists(session, table_name):
    return session.connection().execute(
        f"SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = '{table_name}')"
    ).scalar()

# Function to get the latest date in a table
def get_latest_date(session, table_name):
    latest_date = session.query(func.max(User.datetime)).filter(User.__table__.name == table_name).scalar()
    return latest_date

# Function to create table structure using SQLAlchemy ORM
def create_table(table_name, Base):
    class User(Base):
        __tablename__ = table_name
        id = Column(Integer, primary_key=True, autoincrement=True)
        datetime = Column(Date, nullable=False)
        open = Column(Float)
        high = Column(Float)
        low = Column(Float)
        close = Column(Float)
        volume = Column(Float)

    engine = get_engine_from_settings()
    inspector = inspect(engine)
    if inspector.has_table(table_name):
        User.__table__.drop(engine)
        User.__table__.create(engine)
    else:
        Base.metadata.create_all(engine)
        
    return User

# Function to fetch historical data from TradingView API
def get_historical_data(tv, symbol_exchange_dict, interval, n_bars):
    result = {}
    for symbol, exchange in symbol_exchange_dict.items():
        data = tv.get_hist(symbol=symbol, exchange=exchange, interval=interval, n_bars=n_bars)
        data.reset_index(inplace=True)
        result[symbol] = data
    return result

# Function to insert data into PostgreSQL table
def extract_load_data_to_postgres_db(Base, currency_symbol, historical_data, session):
    name = currency_symbol.lower() + '_' + 'data'
    table_name = name
    User = create_table(table_name, Base)
    logging.info("Start inserting data into {}".format(table_name))
    
    # Convert 'datetime' column to datetime64[ns] objects
    historical_data['datetime'] = pd.to_datetime(historical_data['datetime'])
    
    # Convert latest_date to Timestamp if not None
    latest_date = pd.to_datetime(latest_date) if latest_date is not None else None
    
    new_data = historical_data[historical_data['datetime'] > latest_date] if latest_date is not None else historical_data
    
    session.bulk_insert_mappings(User, new_data.to_dict(orient='records'))
    session.commit()
    logging.info("Data insertion completed at {}".format(datetime.now()))
    logging.info("Program completed successfully.")


# Main program
if __name__ == '__main__':
    username = 'jb0109@protonmail.com'
    password = 'Lambo@01091990'

    tv = TvDatafeed(username, password)

    # Define symbol and exchange dictionary
    symbol_exchange_dict = {
        'XAUUSD': 'OANDA',
        'DXY': 'TVC',
        'USOIL': 'TVC',
        'USINTR': 'ECONOMICS',
        'SPX500USD': 'OANDA'
    }

    # Get historical data for symbols and exchanges in the dictionary
    historical_data = get_historical_data(tv, symbol_exchange_dict, interval=Interval.in_daily, n_bars=10000)

    # Create a SQLAlchemy session
    session = get_session()

    # Iterate over the dictionary items
    for symbol, data in historical_data.items():
        symbol_name, symbol_data = symbol, data

        # Create or check if the table exists
        table_name = symbol_name.lower() + '_data'
        if not table_exists(session, table_name):
            User = create_table(table_name, Base)
            extract_load_data_to_postgres_db(Base, symbol_name, symbol_data, session)
        else:
            latest_date = get_latest_date(session, table_name)

            # Convert latest_date to datetime64[ns]
            latest_date = latest_date if latest_date is None else datetime.combine(latest_date, datetime.min.time())

            new_data = symbol_data[symbol_data['datetime'] > latest_date]
            if not new_data.empty:
                extract_load_data_to_postgres_db(Base, symbol_name, new_data, session)

    # Close the session
    session.close()
