# Author @ Misbah Iradat
"""
    The code is used to extract data from MetaTrader5 using the "MetaTrader5" library 
    and store the data into a database using SQLAlchemy. The code uses a bulk insert 
    approach to insert the data more efficiently into the database. It also logs the 
    program's execution time and status to a log file.

"""

import pandas as pd
import numpy as np
import argparse
import logging
import MetaTrader5 as mt5
from sqlalchemy import Column, Integer,Date, Float
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import inspect
# local files
from session import *
from connect_mt5 import *

# Configure logging for the program
logging.basicConfig(filename='mt5_data_extraction.log', level=logging.DEBUG)
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
        date = Column(Date, nullable=False)
        open = Column(Float)
        high = Column(Float)
        low = Column(Float)
        close = Column(Float)
        tick_volume = Column(Float)
        spread = Column(Float)
        real_volume = Column(Float)
    
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

def extract(Base,currency_symbol, timeframe_val, fromdate, todate):
    
    name = currency_symbol.lower()+'_'+table_names[timeframe_val]
    table_name = name
    # Create SQLAlchemy Base object and User class using the create_table function
    #Base = declarative_base()
    User = create_table(table_name, Base)
    # Create a SQLAlchemy session
    session = Sessions()
    # Get data from the MT5 platform using the get_mt5_data function
    df = get_mt5_data(currency_symbol,timeframe_val, fromdate, todate)
    # Log the start of data insertion into the database
    logging.info("Start inserting data into {}".format(table_name))
    # Insert the data into the database using the bulk_insert_mappings method
    session.bulk_insert_mappings(User,df.to_dict(orient='records'))
    # Commit the transaction to save the changes to the database
    session.commit()
    # Log the completion of data insertion and the successful completion of the program
    logging.info("Data insertion completed at {}".format(datetime.now()))
    logging.info("Program completed successfully.")

if __name__ == '__main__':
    """
    Main program that retrieves data from the MT5 platform and inserts it into the database.
    """
    table_names ={
        "M1": '1minute',
        "M5": '5minute',
        "M15": '15minute',
        "M30": '30minute',
        "H1": '1hour',
        "H4": '4hour',
        "D1": 'daily',
        "W1":'weekly',
        "MN1": 'monthly'
    }

    parser = argparse.ArgumentParser(description='Extract data from MT5 and insert it into database.')

    parser.add_argument('-s', '--symbol', type=str, required=True, help='Currency symbol to retrieve data for.')
    parser.add_argument('-t', '--timeframe', type=str, required=True, help='Timeframe value for data extraction, e.g. mt5.TIMEFRAME_D1.')
    parser.add_argument('-f', '--fromdate', type=str, default='01-01-2002', help='From Date (e.g. 01-01-2002)')
    parser.add_argument('-o', '--todate', type=str, default='31-12-2020', help='To Date (e.g. 31-12-2020)')

    args = parser.parse_args()

    currency_symbol = args.symbol
    timeframe_val = args.timeframe
    fromdate = args.fromdate
    todate = args.todate

    Base = declarative_base()
    extract(Base,currency_symbol, timeframe_val, fromdate, todate)


