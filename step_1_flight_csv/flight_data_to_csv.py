from os import listdir, mkdir, stat
from os.path import isfile, join
from datetime import datetime
from shutil import rmtree

import sys

sys.path.append('..')

import pandas as pd
from shared.dtypes import csv_dtype

IMPORT_FOLDER = '../raw_flight_data'
EXPORT_FOLDER = '../prepared_flight_data'

def import_export_csv_file(import_csv_file_name, export_csv_file_name, start_index):

    print('importing raw csv: {}'.format(import_csv_file_name))

    monthly_flight_data_frame = pd.read_csv(
        import_csv_file_name,
        parse_dates=['FlightDate'],
        dtype=csv_dtype,
        engine='c',
        memory_map=True,
    )
    
    if len(monthly_flight_data_frame.columns) == 110:
        monthly_flight_data_frame.drop(
            monthly_flight_data_frame.columns[109],
            axis=1,
            inplace=True
        )

    print('exporting prepared csv: {}'.format(export_csv_file_name))

    monthly_flight_data_frame.index = monthly_flight_data_frame.index + start_index

    monthly_flight_data_frame.to_csv(
        export_csv_file_name,
        float_format='%.0f',
        header=False,
    )

    return start_index + len(monthly_flight_data_frame)

def main():

    print('preparing starting, start time: {}'.format(datetime.now().time()))

    # create export folder to store prepared data
    rmtree(EXPORT_FOLDER, ignore_errors=True)
    mkdir(EXPORT_FOLDER)


    # load flight data file names
    flight_data_file_names = [ 
        f for f in listdir(IMPORT_FOLDER) 
        if isfile(join(IMPORT_FOLDER, f))
    ]

    start_index = 1

    # iterate over the files importing from the raw csv and
    # exporting the prepared csv
    for file_index, file_name in enumerate(flight_data_file_names):
        # save the last index to use as the first index for the
        # next import
         if file_name.endswith('.csv'):
            start_index = import_export_csv_file(
                join(IMPORT_FOLDER, file_name),
                join(EXPORT_FOLDER, f'flights_{file_index}.csv'),
                start_index
            )
    print('preparing done, stop time: {}'.format(datetime.now().time()))

if __name__ == '__main__':
    main()



