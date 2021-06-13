from os import listdir
from os.path import join, isfile, abspath
import psycopg2
    
def process_query(conn, cursor, sql_code):
    cursor.execute(sql_code)
    conn.commit()

def main():
    
    DROP_FLIGHT_INDEX = 'drop index if exists ix_flight_id;'
    ALTER_FLIGHT_TABLE = 'alter table flight drop constraint if exists flight_pkey;'
    EMPTY_FLIGHT_TABLE = 'delete from flight;'
    COPY_FLIGHT_SQL = "COPY flight FROM stdin WITH DELIMITER ',' CSV;"
    IMPORT_FOLDER = '../prepared_flight_data'
    ADD_PRIMARY_KEY = 'alter table flight add primary key (id)'
    
    # engine used to connect to the database
    conn = psycopg2.connect('postgresql://postgres:testpass@localhost:5432/flights')
    
    with conn.cursor() as cursor:
        
        # build a list of files for the csv files to import into the
        # database
        csv_file_names = [
            abspath(join(IMPORT_FOLDER, file_name)).replace('\\', '/')
                for file_name in listdir(IMPORT_FOLDER)
                if isfile(join(IMPORT_FOLDER, file_name))
        ]
            
        # drop the index and constraint created by the panda schema export to sql
        process_query(conn, cursor, DROP_FLIGHT_INDEX)
        process_query(conn, cursor, ALTER_FLIGHT_TABLE)
        
        # ensure the flight table is empty
        process_query(conn, cursor, EMPTY_FLIGHT_TABLE)
        
        # use postgresql copy command to copy the prepared csv files
        # into the database
        for csv_file_name in csv_file_names:
            print(f'copying {csv_file_name}')
            with open(csv_file_name, 'r') as f:
                cursor.copy_expert(sql=COPY_FLIGHT_SQL, file=f)
                conn.commit()

        # add the primary key to the table
        process_query(conn, cursor, ADD_PRIMARY_KEY)
        
if __name__ == '__main__':
    main()