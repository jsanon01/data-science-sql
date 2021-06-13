import sys

sys.path.append('..')

import pandas as pd
from sqlalchemy import create_engine, types as sql_types

from shared.conversion_utils import get_convert_to_date, get_convert_to_float, get_convert_to_string
from shared.csv_preprocessor import CSVPreprocessor


csv_dtypes = {
    'valid': object,
}

csv_converters = {
    'station': get_convert_to_string('station'),
    'lon': get_convert_to_float('lon'), 
    'lat': get_convert_to_float('lat'),
    'elevation': get_convert_to_float('elevation'),
    'tmpf': get_convert_to_float('tmpf'),
    'dwpf': get_convert_to_float('dwpf'), 
    'relh': get_convert_to_float('relh'), 
    'drct': get_convert_to_float('drct'),
    'sknt': get_convert_to_float('sknt'),
    'p01i': get_convert_to_float('p01i'),
    'alti': get_convert_to_float('alti'),
    'mslp': get_convert_to_float('mslp'),
    'vsby': get_convert_to_float('vsby'),
    'gust': get_convert_to_float('gust'),
    'skyc1': get_convert_to_string('skyc1'),
    'skyc2': get_convert_to_string('skyc2'),
    'skyc3': get_convert_to_string('skyc3'),
    'skyc4': get_convert_to_string('skyc4'),
    'skyl1': get_convert_to_float('skyl1'),
    'skyl2': get_convert_to_float('skyl2'),
    'skyl3': get_convert_to_float('skyl3'),
    'skyl4': get_convert_to_float('skyl4'),
    'wxcodes': get_convert_to_string('wxcodes'),
    'ice_accretion_1hr': get_convert_to_string('ice_accretion_1hr'),
    'ice_accretion_3hr': get_convert_to_string('ice_accretion_3hr'),
    'ice_accretion_6hr': get_convert_to_string('ice_accretion_6hr'),
    'peak_wind_gust': get_convert_to_string('peak_wind_gust'),
    'peak_wind_drct': get_convert_to_string('peak_wind_drct'),
    'peak_wind_time': get_convert_to_string('peak_wind_time'),
    'feel': get_convert_to_float('feel'),
    'metar': get_convert_to_string('metar'),
}

db_dtypes = {
    'station': sql_types.String,
    'lon': sql_types.Float, 
    'lat': sql_types.Float,
    'elevation': sql_types.Float,
    'tmpf': sql_types.Float,
    'dwpf': sql_types.Float, 
    'relh': sql_types.Float, 
    'drct': sql_types.Integer,
    'sknt': sql_types.Integer,
    'p01i': sql_types.Float,
    'alti': sql_types.Float,
    'mslp': sql_types.Float,
    'vsby': sql_types.Integer,
    'gust': sql_types.Integer,
    'skyc1': sql_types.String,
    'skyc2': sql_types.String,
    'skyc3': sql_types.String,
    'skyc4': sql_types.String,
    'skyl1': sql_types.Integer,
    'skyl2': sql_types.Integer,
    'skyl3': sql_types.Integer,
    'skyl4': sql_types.Integer,
    'wxcodes': sql_types.String,
    'ice_accretion_1hr': sql_types.String,
    'ice_accretion_3hr': sql_types.String,
    'ice_accretion_6hr': sql_types.String,
    'peak_wind_gust': sql_types.String,
    'peak_wind_drct': sql_types.String,
    'peak_wind_time': sql_types.String,
    'feel': sql_types.Float,
    'metar': sql_types.String,
}

def main():

    try:

        # load the csv file into a custom preprocessor which will clean up
        # header fields
        data = CSVPreprocessor(open('../weather_data/atlanta_international_airport.csv').detach())

        
        # read weather data
        df = pd.read_csv(
            data,
            dtype=csv_dtypes,
            converters=csv_converters,
            parse_dates=['valid'],
        )

        # engine used by pandas (which use sqlalchemy) to connect to the database
        engine = create_engine('postgresql://postgres:testpass@localhost:5432/flights')
        
        # output weather data to the database
        df.to_sql(
            'weather',
            engine,
            dtype=db_dtypes,
            if_exists='replace',
            index_label='id',
        )

    except Exception as err:
        print('Errors occurred: {}'.format(err))


if __name__ == '__main__':
    main()