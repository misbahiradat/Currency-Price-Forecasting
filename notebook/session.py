# Author @ Misbah Iradat
# This script is using SQLAlchemy, a Python library for working with databases. It has several functions:

"""
            get_engine: This function takes in the details for connecting to a PostgreSQL database 
            (username, password, host, port, and database name),and creates a new database if it 
            doesn't already exist. It then returns an SQLAlchemy engine object.

            get_engine_from_settings: This function imports the settings variable from a separate 
            engine_pass module, and uses it to call the get_engine function. 
            This allows the script to use a separate file for storing the connection details, 
            rather than having them hardcoded in the script.

            get_session: This function calls get_engine_from_settings to get an engine, and then creates 
            and returns a new SQLAlchemy session object, bound to that engine.
            
            The if __name__ == '__main__': block at the end of the script creates a new session and prints it out.

"""

from sqlalchemy import create_engine
from sqlalchemy_utils import database_exists, create_database
from sqlalchemy.orm import sessionmaker
# import the credential file for connecting to postgresql
#from engine_pass import postgresql as settings
from credential import postgresql as settings

def get_engine(user, passwd, host, port, db):
    """
    This function creates an SQLAlchemy engine.

    Args:
        user (str): The username for the database.
        passwd (str): The password for the database.
        host (str): The host for the database.
        port (int): The port for the database.
        db (str): The database name.

    Returns:
        engine (SQLAlchemy Engine): The SQLAlchemy engine.

    """
    url = f"postgresql://{user}:{passwd}@{host}:{port}/{db}"

    # check if the database exists, and create it if it doesn't
    if not database_exists(url):
        create_database(url)
    engine = create_engine(url, pool_size=50, echo=False)
    return engine

def get_engine_from_settings():
    """
    This function creates an SQLAlchemy engine using the settings from 'engine_pass.py'.

    Returns:
        engine (SQLAlchemy Engine): The SQLAlchemy engine.

    Raises:
        Exception: If the required keys are not present in the config file.

    """
    keys = ['pguser', 'pgpass', 'host', 'port', 'pgdb']

    # raise an exception if the required keys are not present in the config file
    if not all(key in keys for key in settings.keys()):
        raise Exception('Bad config file')

    return get_engine(settings['pguser'], 
                    settings['pgpass'], 
                    settings['host'], 
                    settings['port'], 
                    settings['pgdb'])

def get_session():
    """
    This function creates an SQLAlchemy session.

    Returns:
        session (SQLAlchemy Session): The SQLAlchemy session.

    """
    engine = get_engine_from_settings()
    session = sessionmaker(bind=engine)()
    return session

if __name__ == '__main__':
    # create a session and print it
    session = get_session()
    print(session)