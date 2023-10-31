# Author @ Misbah Iradat
import sys
sys.path.append('../script')

import logging
import os
import pandas as pd
import pytest
from sqlalchemy import create_engine, MetaData, Table
from sqlalchemy.orm import sessionmaker
from datetime import datetime, date



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

@pytest.fixture(scope="module")
def db():
    """
    Fixture to set up a database connection and create a table
    """
    # Create an in-memory SQLite database
    engine = create_engine('sqlite:///:memory:')

    # Create a table using SQLAlchemy ORM
    metadata = MetaData()
    table_name = 'test_table'
    table = Table(table_name, metadata,
                  # Use a custom primary key to avoid auto-incrementing
                  # which can cause issues with test runs
                  Column('id', Integer, primary_key=True),
                  Column('date', Date, nullable=False),
                  Column('open', Float),
                  Column('high', Float),
                  Column('low', Float),
                  Column('close', Float),
                  Column('tick_volume', Float),
                  Column('spread', Float),
                  Column('real_volume', Float)
                  )
    metadata.create_all(engine)

    # Return the database connection and table object
    yield (engine, table_name)

    # Clean up by dropping the table and closing the connection
    metadata.drop_all(engine)
    engine.dispose()

def test_extract(db):
    """
    Test that data is extracted and inserted correctly
    """
    # Unpack the database connection and table name from the fixture
    engine, table_name = db

    # Set up logging
    logging.basicConfig(filename='test.log', level=logging.DEBUG)

    # Set up test data
    currency_symbol = 'EURUSD'
    timeframe_val = 'M1'
    fromdate = '01-01-2022'
    todate = '31-01-2022'

    # Call the extract function with test data
    Base = declarative_base()
    extract(Base, currency_symbol, timeframe_val, fromdate, todate)

    # Verify that data was inserted into the table
    conn = engine.connect()
    result = conn.execute(f'SELECT COUNT(*) FROM {table_name}')
    assert result.fetchone()[0] > 0

    # Verify that data was inserted with the correct values
    result = conn.execute(f'SELECT * FROM {table_name}')
    data = pd.DataFrame(result.fetchall(), columns=result.keys())
    assert not data.empty
    assert data['date'].dtype == 'datetime64[ns]'
    assert not data.isnull().values.any()

    # Verify that the log file was created and contains expected logs
    assert os.path.exists('test.log')
    with open('test.log', 'r') as f:
        log_lines = f.readlines()
        assert 'Program started at' in log_lines[0]
        assert 'Data insertion completed at' in log_lines[-2]
        assert 'Program completed successfully' in log_lines[-1]
