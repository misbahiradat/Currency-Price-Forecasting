# Author @ Misbah Iradat
from tvDatafeed import TvDatafeed, Interval

import pandas as pd
import numpy as np
import argparse
import logging
from sqlalchemy import Column, Integer,Date, Float
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import inspect
# local files
from session import *
from connect_mt5 import *



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
    Base.metadata.create_all(bind=engine)
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
    class User(Base):
        __tablename__ = table_name
        id = Column(Integer, primary_key=True, autoincrement=True)
        datetime = Column(Date, nullable=False)
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
        data = tv.get_hist(symbol=symbol, exchange=exchange, interval=interval, n_bars=n_bars)
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
    session.bulk_insert_mappings(User,historical_data.to_dict(orient='records'))
    # Commit the transaction to save the changes to the database
    session.commit()
    # Log the completion of data insertion and the successful completion of the program
    logging.info("Data insertion completed at {}".format(datetime.now()))
    logging.info("Program completed successfully.")



# def main(Base):
#     username = settings['username']
#     password = settings['password']

#     tv = TvDatafeed(username, password)

#     # Define symbol and exchange dictionary
#     symbol_exchange_dict = {
#         'XAUUSD': 'OANDA',
#         'DXY': 'TVC',
#         'USOIL': 'TVC',
#         'USINTR': 'ECONOMICS',
#         'SPX500USD': 'OANDA'
#     }

#     # Get historical data for symbols and exchanges in the dictionary
#     historical_data = get_historical_data(tv, symbol_exchange_dict, interval=Interval.in_daily, n_bars=10000)

#     # Access individual dataframes
#     gold_symbol, gold_data = historical_data['XAUUSD']
#     extract_load_data_to_postgres_db(Base,gold_symbol,gold_data)

#     dollarIndex_symbol, dollarIndex_data = historical_data['DXY']
#     extract_load_data_to_postgres_db(Base,dollarIndex_symbol,dollarIndex_data)

#     oil_symbol, oil_data = historical_data['USOIL']
#     extract_load_data_to_postgres_db(Base,oil_symbol,oil_data)

#     interestrate_symbol,interestrate_data = historical_data['USINTR']
#     extract_load_data_to_postgres_db(Base,interestrate_symbol,interestrate_data)

#     sp500_symbol,SP500 = historical_data['SPX500USD']
#     extract_load_data_to_postgres_db(Base,sp500_symbol,SP500)


settings = {'username':'*******',
                'password':'********'}

#if __name__ == '__main__':
Base = declarative_base()
username = settings['username']
password = settings['password']

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

# Iterate over the dictionary items
for symbol, data in historical_data.items():
    # Access individual symbol and data
    symbol_name, symbol_data = symbol, data
    # Call the function to extract and load data to the database
    extract_load_data_to_postgres_db(Base,symbol_name, symbol_data)
# # Access individual dataframes
# gold_symbol, gold_data = historical_data['XAUUSD']
# extract_load_data_to_postgres_db(Base,gold_symbol,gold_data)

# dollarIndex_symbol, dollarIndex_data = historical_data['DXY']
# extract_load_data_to_postgres_db(Base,dollarIndex_symbol,dollarIndex_data)

# oil_symbol, oil_data = historical_data['USOIL']
# extract_load_data_to_postgres_db(Base,oil_symbol,oil_data)

# interestrate_symbol,interestrate_data = historical_data['USINTR']
# extract_load_data_to_postgres_db(Base,interestrate_symbol,interestrate_data)

# sp500_symbol,SP500 = historical_data['SPX500USD']
# extract_load_data_to_postgres_db(Base,sp500_symbol,SP500)
