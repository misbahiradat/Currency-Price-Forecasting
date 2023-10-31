# MT5 Data Extraction

This script is used to extract data from the [MetaTrader 5 (MT5)](https://www.metatrader5.com/en/download) platform and insert it into a database using SQLAlchemy. 
The data can be extracted for any currency symbol and time frame (e.g. 1 minute, 5 minute, daily, etc.).

## Prerequisites

- You need to have a MetaTrader 5 account.
- You need to install the MetaTrader 5 and SQLAlchemy Python packages.

## Usage

The script can be executed from the command line with the following arguments:

```ruby
 -s, --symbol: Currency symbol to retrieve data for. (required)
 -t, --timeframe: Timeframe value for data extraction, 
 e.g. timeframe_mapping = {"M1": mt5.TIMEFRAME_M1, 
        "M5": mt5.TIMEFRAME_M5,
        "M15": mt5.TIMEFRAME_M5,
        "M30": mt5.TIMEFRAME_M30,
        "H1": mt5.TIMEFRAME_H1,
        "H4": mt5.TIMEFRAME_H4,
        "D1": mt5.TIMEFRAME_D1,
        "W1": mt5.TIMEFRAME_W1,
        "MN1": mt5.TIMEFRAME_MN1}. (required)
 -f, --fromdate: From Date (e.g. 01-01-2002) (optional, default: 01-01-2002)
 -o, --todate: To Date (e.g. 31-12-2020) (optional, default: 31-12-2020)
```
### Example
##### Extract and store it to local (postgreSQL)
To extract daily data for the currency symbol EURUSD from 01-01-2002 to 31-12-2020, and store it on a local postgreSQL then run the following command:

```ruby
python data_extraction.py -s EURUSD -t D1 -f 01-01-2002 -o 31-12-2020
```
  To access the data from postgreSQL, run the following command:

  ```ruby
  psql -U {USERNAME} -d {database_name}
  ```

To extract daily data for the currency symbol EURUSD from 01-01-2002 to 31-12-2020, and store it on a local csv file then run the following command:
##### Extract and store it to local directory (.csv)
```ruby
python connect_mt5.py -s EURUSD -t D1 -f 01-01-2002 -o 31-12-2020
```

## Logging

The script logs all important events in the file mt5_data_extraction.log in the same directory as the script.

## Note
if you want to save the data to postgreSQL, then make sure you have other python scripts availabel in the directory : session.py & connect_mt5.py and engine_pass.py
- The script assumes that you have a database set up and configured the connection settings in the session.py file.
- The script creates a table for each currency symbol and time frame combination in the database, using the naming convention [currency_symbol]_[timeframe_name].

