from os import listdir
from os.path import isfile, join
from datetime import datetime
import sys

sys.path.append('..')

from sqlalchemy import create_engine
import pandas as pd
from shared.dtypes import csv_dtype, db_dtype
from shared.utils import convert

IMPORT_FOLDER = '../raw_flight_data'

def main():

    print('configuring flight schema, start time: {}'.format(datetime.now().time()))

    # engine used by pandas (which use sqlalchemy) to connect to the database
    engine = create_engine('postgresql://postgres:testpass@localhost:5432/flights')

    # load a single flight csv file
    flight_data_file_name = join(IMPORT_FOLDER, [
        f for f in listdir(IMPORT_FOLDER)
        if isfile(join(IMPORT_FOLDER, f)) and f.startswith('On')
    ][0])

    print('import schema from csv')

    # read the csv file into a data frame
    flight_data_frame = pd.read_csv(
        flight_data_file_name,
        parse_dates=['FlightDate'],
        dtype=csv_dtype,
        nrows=1,
        engine='c',
        memory_map=True,
    )

    # convert column names to the underscore names for
    # database table
    flight_data_frame.columns = [
        convert(column) for column in flight_data_frame.columns
    ]

    # drop last column
    if len(flight_data_frame.columns) == 110:
        flight_data_frame.drop(
            flight_data_frame.columns[109],
            axis=1,
            inplace=True
        )

    print('exporting schema to postgresql')

    # slice off all of the data, export the schema only to the
    # database
    flight_data_frame[0:0].to_sql(
        'flight',
        engine,
        dtype=db_dtype,
        if_exists='replace',
        index_label='id',
    )

    print('configuring flight schema, stop time: {}'.format(datetime.now().time()))


if __name__ == '__main__':
    main()    



