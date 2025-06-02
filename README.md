# Corteva Code Challenge
This is for the Corteva Code Challenge for Mallory Row for the Senior Atmospheric Data Scientist position.

## About
This processes daily weather data for stations in Nebraska, Iowa, Illinois, Indiana, or Ohio. It takes those daily files and processes them for every year and for every weather station to calculate certain statistics. After the data is processesed, an API can be run locally to filter and look through the data.

## Requirements
### Python Libraries
The following python libraries are used:
```
os
sys
logging
datetime
pandas
glob
sqlalchemy
sqlite3
flask
flasgger
```
### Input Data
Additionally text files in the `wx_data` directory are needed. Each file corresponds to a particular weather station from Nebraska, Iowa, Illinois, Indiana, or Ohio. The file name contains the weather station's identification.

Each line in the file contains 4 records separated by tabs: 
1. The date (YYYYMMDD format)
2. The maximum temperature for that day (in tenths of a degree Celsius)
3. The minimum temperature for that day (in tenths of a degree Celsius)
4. The amount of precipitation for that day (in tenths of a millimeter)

Missing values are indicated by the value -9999.

## Running
Everything needed to run is in the `src` directory and should be run from there.
1. The data processing script is `wx_data.py`. Run the script (`python wx_data.py`). This will generate 3 files in the main directory
     - <b>raw_wx_data.db</b>: This is the database the contains of the daily weather data: the date (YYYYMMDD format), daily maximum temperature (degree Celsius), daily minimum temperature (degree Celsius), precipitation (millimeter), and station ID.
     - <b>stats_wx_data.db</b>: This is the database the contains of the annual weather data: yearly average maximum temperature (degree Celsius), yearly average minimum temperature (degree Celsius), precipitation (centimeter), station ID, and year (YYYY format).
     - <b>wx_data_run<i>YYYYmmdd_HHMMSS</i>.log</b>: This is the log file for the run, where <i>YYYYmmdd_HHMMSS</i> is the time `wx_data.py` was executed.

2. The script to generate the locally run API is `app.py`. Run the script (`python app.py`). The API can be accesssed by opening a web browsers and using `http://127.0.0.1:5000/`.

## Interacting with the API
The API has 2 GET endpoints and an endpoint for Swagger documentation

### api/weather
```
This reads the daily weather data from raw_wx_data.db.

A query can be done using ? after the URL; for more than one query use & between the entries:
- StationID: station's identification
- Date: date in format of YYYYmmdd
- limit: the number of records to show (default is 10)
- offset: the record to start at (default is 0)

Example: http://127.0.0.1:5000/api/weather/?StationID=USC00129670&Date=19850102
```
  
### api/weather/stats
```
This reads the annual weather data from stats_wx_data.db.

A query can be done using ? after the URL; for more than one query use & between the entries:
- StationID: station's identification
- Year: year in format of YYYY
- limit: the number of records to show (default is 10)
- offset: the record to start at (default is 0)

Example: http://127.0.0.1:5000/api/weather/stats/?StationID=USC00129670&Year=2010
```

### apidocs
```
This contains basic documentation about the API.
```
