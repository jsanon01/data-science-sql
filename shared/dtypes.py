from sqlalchemy import types as sql_types

csv_dtype = {
    "Year": float,
    "Quarter": float,
    "Month": float,
    "DayofMonth": float,
    "DayOfWeek": float,
    "FlightDate": object,
    "UniqueCarrier": object,
    "AirlineID": float,
    "Carrier": object,
    "TailNum": object,
    "FlightNum": float,
    "OriginAirportID": float,
    "OriginAirportSeqID": float,
    "OriginCityMarketID": float,
    "Origin": object,
    "OriginCityName": object,
    "OriginState": object,
    "OriginStateFips": float,
    "OriginStateName": object,
    "OriginWac": float,
    "DestAirportID": float,
    "DestAirportSeqID": float,
    "DestCityMarketID": float,
    "Dest": object,
    "DestCityName": object,
    "DestState": object,
    "DestStateFips": float,
    "DestStateName": object,
    "DestWac": float,
    "CRSDepTime": float,
    "DepTime": float,
    "DepDelay": float,
    "DepDelayMinutes": float,
    "DepDel15": float,
    "DepartureDelayGroups": float,
    "DepTimeBlk": object,
    "TaxiOut": float,
    "WheelsOff": float,
    "WheelsOn": float,
    "TaxiIn": float,
    "CRSArrTime": float,
    "ArrTime": float,
    "ArrDelay": float,
    "ArrDelayMinutes": float,
    "ArrDel15": float,
    "ArrivalDelayGroups": float,
    "ArrTimeBlk": object,
    "Cancelled": bool,
    "CancellationCode": object,
    "Diverted": bool,
    "CRSElapsedTime": float,
    "ActualElapsedTime": float,
    "AirTime": float,
    "Flights": float,
    "Distance": float,
    "DistanceGroup": float,
    "CarrierDelay": float,
    "WeatherDelay": float,
    "NASDelay": float,
    "SecurityDelay": float,
    "LateAircraftDelay": float,
    "FirstDepTime": float,
    "TotalAddGTime": float,
    "LongestAddGTime": float,
    "DivAirportLandings": float,
    "DivReachedDest": float,
    "DivActualElapsedTime": float,
    "DivArrDelay": float,
    "DivDistance": float,    
}

db_dtype = {
    "year": sql_types.Integer,
    "quarter": sql_types.Integer,
    "month": sql_types.Integer,
    "dayof_month": sql_types.Integer,
    "day_of_week": sql_types.Integer,
    "airline_id": sql_types.Integer,
    "flight_num": sql_types.Integer,
    "origin_airport_id": sql_types.Integer,
    "origin_airport_seq_id": sql_types.Integer,
    "origin_city_market_id": sql_types.Integer,
    "origin_state_fips": sql_types.Integer,
    "origin_wac": sql_types.Integer,
    "dest_airport_id": sql_types.Integer,
    "dest_airport_seq_id": sql_types.Integer,
    "dest_city_market_id": sql_types.Integer,
    "dest_state_fips": sql_types.Integer,
    "dest_wac": sql_types.Integer,
    "crs_dep_time": sql_types.Integer,
    "dep_time": sql_types.Integer,
    "dep_delay": sql_types.Integer,
    "dep_delay_minutes": sql_types.Integer,
    "dep_del15": sql_types.Integer,
    "departure_delay_groups": sql_types.Integer,
    "taxi_out": sql_types.Integer,
    "wheels_off": sql_types.Integer,
    "wheels_on": sql_types.Integer,
    "taxi_in": sql_types.Integer,
    "crs_arr_time": sql_types.Integer,
    "arr_time": sql_types.Integer,
    "arr_delay": sql_types.Integer,
    "arr_delay_minutes": sql_types.Integer,
    "arr_del15": sql_types.Integer,
    "arrival_delay_groups": sql_types.Integer,
    "cancellation": sql_types.Integer,
    "crs_elapsed_time": sql_types.Integer,
    "actual_elapsed_time": sql_types.Integer,
    "air_time": sql_types.Integer,
    "flights": sql_types.Integer,
    "distance": sql_types.Integer,
    "distance_group": sql_types.Integer,
    "carrier_delay": sql_types.Integer,
    "weather_delay": sql_types.Integer,
    "nas_delay": sql_types.Integer,
    "security_delay": sql_types.Integer,
    "late_aircraft_delay": sql_types.Integer,
    "first_dep_time": sql_types.Integer,
    "total_add_g_time": sql_types.Integer,
    "longest_add_g_time": sql_types.Integer,
    "div_airport_landings": sql_types.Integer,
    "div_reached_dest": sql_types.Integer,
    "div_actual_elapsed_time": sql_types.Integer,
    "div_arr_delay": sql_types.Integer,
    "div_distance": sql_types.Integer,    
}

for num in range(1,6):

    csv_dtype["Div{}Airport".format(num)] = object
    csv_dtype["Div{}AirportID".format(num)] = float
    csv_dtype["Div{}AirportSeqID".format(num)] = float
    csv_dtype["Div{}WheelsOn".format(num)] = float
    csv_dtype["Div{}TotalGTime".format(num)] = float
    csv_dtype["Div{}LongestGTime".format(num)] = float
    csv_dtype["Div{}WheelsOff".format(num)] = float
    csv_dtype["Div{}TailNum".format(num)] = object

    db_dtype["div{}_airport".format(num)] = sql_types.Text
    db_dtype["div{}_airport_id".format(num)] = sql_types.Integer
    db_dtype["div{}_airport_seq_id".format(num)] = sql_types.Integer
    db_dtype["div{}_wheels_on".format(num)] = sql_types.Integer
    db_dtype["div{}_total_g_time".format(num)] = sql_types.Integer
    db_dtype["div{}_longest_g_time".format(num)] = sql_types.Integer
    db_dtype["div{}_wheels_off".format(num)] = sql_types.Integer
    db_dtype["div{}_tail_num".format(num)] = sql_types.Text    
