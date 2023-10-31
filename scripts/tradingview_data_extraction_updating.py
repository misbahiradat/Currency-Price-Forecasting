# Author @ Misbah Iradat
from tvDatafeed import TvDatafeed, Interval

import pandas as pd
import numpy as np
import argparse
import logging
from sqlalchemy import Table, Column, Integer, Date, String, Float
from sqlalchemy.ext.declarative import declarative_base
import pandas as pd
from datetime import datetime
from sqlalchemy import inspect
from sqlalchemy import insert
# local files
from session import *
from datetime import datetime, date 
from credential import postgresql as settings
from credential import tradingview as tv_settings


# Configure logging for the program
logging.basicConfig(filename='tradingview_data_extraction.log', level=logging.DEBUG)
logging.info("Program started at {}".format(datetime.now()))

def Sessions():
    """
    Function to create the SQLAlchemy engine and session
    Returns:
        session object
    """
    engine = get_engine_from_settings()
    DynamicBase.metadata.create_all(bind=engine)
    session = get_session()
    return session

# datetime	symbol	open	high	low	close	volume
def create_table(table_name, Base):
    """
    Function to create table structure using SQLAlchemy ORM
    Args:
        table_name : name of the table to be created
        Base       : SQLAlchemy Base object
        engine     : SQLAlchemy engine object
    Returns:
        User       : SQLAlchemy ORM Class for the table
    """
    
    engine = get_engine_from_settings()
    #DynamicBase = declarative_base(class_registry=dict())
    class User(DynamicBase):
            __tablename__ = table_name
            id = Column(Integer, primary_key=True, autoincrement=True)
            datetime = Column(Date) #nullable=False
            open = Column(Float)
            high = Column(Float)
            low = Column(Float)
            close = Column(Float)
            volume = Column(Float)
        
    inspector = inspect(engine)
    #if engine.has_table(table_name):
    if inspector.has_table(table_name):
        # if table exists, overwrite it
        User.__table__.drop(engine)
        User.__table__.create(engine)
    else:
        # if table does not exist, create it
        Base.metadata.create_all(engine)
        
    return User

def get_historical_data(tv, symbol_exchange_dict, interval, n_bars):
    result = {}
    for symbol, exchange in symbol_exchange_dict.items():
        if not symbol == 'USCCPI':
            data = tv.get_hist(symbol=symbol, exchange=exchange, interval=interval, n_bars=n_bars)
            data.reset_index(inplace=True)
            result[symbol] = data
        else:
            data = tv.get_hist(symbol=symbol, exchange=exchange,n_bars=500)
            data.reset_index(inplace=True)
            result[symbol] = data

    return result

def extract_load_data_to_postgres_db(Base,currency_symbol,historical_data):

    name = currency_symbol.lower()+'_'+'data'
    table_name = name
    # Create SQLAlchemy Base object and User class using the create_table function
    #Base = declarative_base()
    User = create_table(table_name, Base)
    # Create a SQLAlchemy session
    session = Sessions()

    # Log the start of data insertion into the database
    logging.info("Start inserting data into {}".format(table_name))
    # Insert the data into the database using the bulk_insert_mappings method
    #session.bulk_insert_mappings(User,historical_data.to_dict(orient='records'))
    session.bulk_insert_mappings(User,historical_data.to_dict(orient='records'))
    # Commit the transaction to save the changes to the database
    session.commit()
    # Log the completion of data insertion and the successful completion of the program
    logging.info("Data insertion completed at {}".format(datetime.now()))
    logging.info("Program completed successfully.")



def get_latest_date(session, table_name):
    # Define your SQL query to select the latest date from the table
    sql_query = f"SELECT max(datetime) FROM {table_name} LIMIT 5"
    #max(datetime)
    # Execute the query and fetch the result
    result = session.connection().execute(sql_query)
    
    # Fetch the first row (which contains the latest date)
    latest_date = result.fetchone()[0]

    #Check if the latest_date is not None, and then format and print it
    if latest_date:
        formatted_date = latest_date.strftime('%Y-%m-%d')
        return formatted_date
    else:
        return ("No data found in the table.")
    

# Function to check if a table exists in the database
def table_exists(session, table_name):
    return session.connection().execute(
        f"SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = '{table_name}')"
    ).scalar()


def load_data_to_postgres_db(Base,currency_symbol,historical_data, session):
    
    name = currency_symbol.lower()+'_'+'data'
    table_name = name
    # Create SQLAlchemy Base object and User class using the create_table function
    User = create_table(table_name, Base)
    # Log the start of data insertion into the database
    logging.info("Start inserting data into {}".format(table_name))
    # Insert the data into the database using the bulk_insert_mappings method
    session.bulk_insert_mappings(User,historical_data.to_dict(orient='records'))
    # Commit the transaction to save the changes to the database
    session.commit()
    # Log the completion of data insertion and the successful completion of the program
    logging.info("Data insertion completed at {}".format(datetime.now()))
    logging.info("Program completed successfully.")



def process_historical_data(tv, symbol_exchange_dict, settings):
    """
    Process historical data for multiple symbols and store it in a PostgreSQL database.

    Args:
        tv: TradingView object or module for fetching data.
        symbol_exchange_dict (Dict[str, str]): A dictionary mapping symbols to exchanges.
        settings (dict): A dictionary containing PostgreSQL database connection settings.

    Returns:
        None
    """

    session = Sessions()
    # initializing the dictionary
    historical_data = {}
    new_historical_data = {}
    # Iterate over the dictionary items
    for symbol in symbol_exchange_dict.keys():
        symbol_name = symbol
        # creating table name
        table_name = symbol_name.lower() + '_data'
        # checking if table exists or not!
        if not table_exists(session, table_name):
            historical_data = get_historical_data(tv, symbol_exchange_dict, interval=Interval.in_daily, n_bars=10000)
        else:
            # if table exists, then we extract the last updated data and extract the date!
            latest_date = get_latest_date(session, table_name)
            # converting the date to the correct format!
            latest_date = pd.to_datetime(latest_date)
            # getting the last 100 day data!
            new_historical_data = get_historical_data(tv, symbol_exchange_dict, interval=Interval.in_daily, n_bars=100)

    if len(historical_data) > 0:
        for symbol, data in historical_data.items():
            symbol_name, symbol_data = symbol, data
            # loading the data to the postgres database!
            load_data_to_postgres_db(DynamicBase ,symbol_name,symbol_data, session)
            logging.info(f"Loaded historical data for {symbol_name} into the database.")
    else:
        for symbol, data in new_historical_data.items():
            symbol_name, symbol_data = symbol, data
            table_name = symbol_name.lower() + '_data'
            data = symbol_data.loc[symbol_data['datetime'].dt.date > latest_date.date()]
            if 'index' in data.columns:
                data = data.drop(columns=['index'])
            data.loc[:, 'datetime'] = data['datetime'].dt.date
            data = data.drop(columns=['symbol'])
            # updating the table with new data!
            data.to_sql(table_name, con= get_engine(settings['pguser'], 
                            settings['pgpass'], 
                            settings['host'], 
                            settings['port'], 
                            settings['pgdb']), if_exists='append', index=False)
            logging.info(f"Appended new historical data for {symbol_name} into the database.")

if __name__ == '__main__':
    DynamicBase = declarative_base()

    username = tv_settings['username']
    password = tv_settings['password']
    tv = TvDatafeed(username, password)


    # Define symbol and exchange dictionary
    symbol_exchange_dict = {
        'XAUUSD': 'OANDA',
        'DXY': 'TVC',
        'USOIL': 'TVC',
        'USINTR': 'ECONOMICS',
        'USCCPI': 'ECONOMICS',
        'SPX500USD': 'OANDA'
    }

    process_historical_data(tv, symbol_exchange_dict, settings)