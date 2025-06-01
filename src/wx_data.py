"""
About:
    This script reads in weather data text files. It will calculate
    for every year, for every weather station
        1. Average maximum temperature (in degrees Celsius)
        2. Average minimum temperature (in degrees Celsius)
        3. Total accumulated precipitation (in centimeters)

Input File Format:
    Each file is the station name. In each file is
    4 records sepearted by tabs:
        1. Date (YYYYMMDD format)
        2. Maximum temperature for date (in tenths of a degree Celsius)
        3. Minimum temperature for date (in tenths of a degree Celsius)
        4. Precipitation for that day (in tenths of a millimeter)

Author(s):
	Mallory Row (mallory.row@gmail.com)

Input Agruments:
"""

#### Load modules
import os
import sys
import logging
import datetime
import pandas as pd
import glob
from sqlalchemy import create_engine

def have_input_dir_files(dir_path, log):
    """ ! Check to see the provide input directory
          exists and contains files

          Args:
              dir_path - directory path to input data
                         (string)
              log      - file path to the logging file
                         (logger object)
          Returns:
              have_input - successful checks of directory
                           and files returns True
                           if either fail returns False
                           (boolean)
    """
    have_input = False
    log.info(f"Checking input directory: {dir_path} "
                +f"(absolute path: {os.path.abspath(dir_path)})")
    if not os.path.exists(dir_path):
        log.error(f"{dir_path} does not exist\n")
        sys.exit(os.EX_NOINPUT)
    elif len(os.listdir()) == 0:
        log.warning(f"{dir_path} has no files\n")
        sys.exit(os.EX_NOINPUT)
    else:
        log.info("Successfully have input data\n")
        have_input = True
    return have_input

def have_db(db_path, log):
    """! Check to see if database exist

         Args:
              dir_path - directory path to database
                         (string)
              log      - file path to the logging file
                         (logger object)
          Returns:
              have_db - existence of the database file
                        returns True, otherwise False
                        (boolean)
    """
    have_db = False
    if os.path.exists(db_path):
        log.info("{os.path.abspath(db_path)} exists")
    return have_db

def read_txt_wx_data(sid_file, log):
    """ ! Reads a single input text file of the
          following format

          File name is the station name. In each file is
          4 records sepearted by tabs:
              1. Date
                 (YYYYMMDD format)
              2. Maximum temperature for date
                 (in tenths of a degree Celsius)
              3. Minimum temperature for date
                 (in tenths of a degree Celsius)
              4. Precipitation for that day
                 (in tenths of a millimeter)

          Args:
              sid_file - path to the input file with weather
                         data
                         (string)
              log      - file path to the logging file
                         (logger object)
          Returns:
              sid_file_df - contains the station's data
                            (pandas DataFrame)

    """
    sid = sid_file.rpartition('/')[2].split('.')[0]
    try:
        sid_file_df = pd.read_csv(
            sid_file, sep="\t", skipinitialspace=True, header=None,
            names=["Date", "MaxT", "MinT", "Precip"]
        )
        log.debug(f"Reading {sid_file}")
        sid_file_df['StationID'] = len(sid_file_df) * [sid]
        # Missing values are indicated by the value -9999, drop
        index_missing = sid_file_df[
            (sid_file_df['MaxT'] == -9999)
            & (sid_file_df['MinT'] == -9999)
            & (sid_file_df['Precip'] == -9999)
        ].index
        sid_file_df.drop(index_missing, inplace=True)
        # Adjust units
        ## tenths of a degree Celsius -> degree Celsius
        ## tenths of a millimeter -> millimeter
        sid_file_df.MaxT = sid_file_df.MaxT/10.0
        sid_file_df.MinT = sid_file_df.MinT/10.0
        sid_file_df.Precip = sid_file_df.Precip/10.0
    except:
        log.warning(f"Problem reading {sid_file}")
        sid_file_df = pd.DataFrame()
    return sid_file_df

def calculate_stats_wx_data(stations_df, log):
    """ ! Calculates for every year, for every weather station
              1. Average maximum temperature (in degrees Celsius)
              2. Average minimum temperature (in degrees Celsius)
              3. Total accumulated precipitation (in centimeters)

          Args:
              stations_df - pandas DataFrame with columns
                            Date, MaxT, MinT, Precip, StationID
                            (dataframe)
              log         - file path to the logging file
                            (logger object)
          Returns:
              station_year_stats_df - contains the above
                                      mentioned statistics
                                      (pandas DataFrame)
    """
    stations_df['Year'] = [str(y)[0:4] for y in stations_df.Date]
    # Put precipitation from mm to cm
    stations_df.Precip = stations_df.Precip/10.0
    try:
        station_year_stats_df = (
            stations_df.groupby(by=['StationID', 'Year']).aggregate(
                {'MaxT':'mean', 'MinT': 'mean', 'Precip': 'sum'}
            )
        )
        station_year_stats_df.rename(
            columns={'MaxT': 'AvgMaxT',
                     'MinT': 'AvgMinT',
                     'Precip': 'TotalPrecip'},
            inplace=True
        )
    except:
        log.warning("Could not calculate statistics for weather data")
        station_year_stats_df = pd.DataFrame()
    return station_year_stats_df

def main():
    # Set up input directory
    input_dir = '../wx_data'

    # Set up database names
    raw_wx_db = "../raw_wx_data.db"
    stats_wx_db = "../stats_wx_data.db"

    # Set up logging
    log_file = f"../wx_data_run{datetime.datetime.today():%Y%m%d_%H%M%S}.log"
    logger = logging.getLogger(log_file)
    logger.setLevel(logging.INFO)
    formatter = logging.Formatter(
        '%(asctime)s.%(msecs)03d (%(filename)s:%(lineno)d) %(levelname)s: '
        + '%(message)s', '%m/%d %H:%M:%S'
    )
    file_handler = logging.FileHandler(log_file, mode='a')
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    print(f"Log File is at {os.path.abspath(log_file)}")

    # Read and process raw weather data
    if have_input_dir_files(input_dir, logger) \
            and not have_db(raw_wx_db, logger):
        read_input_data = True
    else:
        read_input_data = False
    if read_input_data:
        logger.info(f"Processing raw weather data")
        all_station_id_data = []
        logger.info(f"Started reading files {datetime.datetime.today()}")
        for station_id_file in glob.glob(os.path.join(input_dir, '*.txt')):
            station_id_df = read_txt_wx_data(station_id_file, logger)
            if not station_id_df.empty:
                for d in station_id_df.values.tolist():
                    all_station_id_data.append(d)
        all_station_df = pd.DataFrame(
            all_station_id_data, columns=["Date", "MaxT", "MinT",
                                          "Precip", "StationID"]
        )
        logger.info(f"Finished reading files at {datetime.datetime.today()}")
        all_station_df_recs = len(all_station_df)
        logger.info(f"There are {all_station_df_recs} total records")
        # Put raw weather data in database
        logger.info("Loading raw weather data into database "
                    +f"{os.path.abspath(raw_wx_db)}")
        raw_wx_engine = create_engine('sqlite:///'+raw_wx_db)
        all_station_df.to_sql('WX_Data_Raw', raw_wx_engine, if_exists='replace',
                              index=False)

        # Calculate statistics
        if not have_db(stats_wx_db, logger):
            if all_station_df_recs != 0:
                calculate_stats = True
            else:
                logger.warning("Raw weather dataframe empty, "
                               +"not calculating stats")
                calculate_stats = False
            logger.info("Calculating Station Yearly average maximum temperature "
                        +"average minimum temperature, and total precipitation")
            calc_stats_df = calculate_stats_wx_data(all_station_df, logger)
            # Put weather stats data in database
            logger.info("Loading weather stats data into database "
                        +f"{os.path.abspath(stats_wx_db)}")
            stats_wx_engine = create_engine('sqlite:///'+stats_wx_db)
            calc_stats_df.to_sql('WX_Data_Stats', stats_wx_engine,
                                 if_exists='replace')

if __name__ == "__main__":
    main()
