from os import listdir
from os.path import isfile, join
import re
import threading

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import pandas as pd

IMPORT_FOLDER = '../raw_lookup_data'

class ImportLookupThread(threading.Thread):

    def __init__(self, thread_id, table_name, csv_file_name, session):
        threading.Thread.__init__(self)
        self.thread_id = thread_id
        self.csv_file_name = csv_file_name
        self.table_name = table_name
        self.session = session
        print('import thread of table {} initialized'.format(self.table_name))

    def run(self):
        print('import thread of table {} started'.format(self.table_name))
        import_lookup_table(self.table_name, self.csv_file_name, self.session)
        print('import thread of table {} complete'.format(self.table_name))

def import_lookup_table(table_name, csv_file_name, session):
    
    print("importing from {}".format(csv_file_name))
    
    # read lookup table csv into a data frame
    df = pd.read_csv(csv_file_name)

    # rename column for database
    df.columns = [column.lower() for column in df.columns]
    
    print("exporting to {}".format(table_name))

    # set starting index to 1
    df.index = df.index + 1
    # export to sql database
    df.to_sql(
        table_name,
        session.get_bind(),
        if_exists="replace",
        index_label="id",
        chunksize=1000
    )

    print("add indexes to {}".format(table_name))

    # drop default index
    session.execute('drop index ix_{}_id'.format(table_name))

    # add primary key
    session.execute('alter table {} add primary key (id)'.format(table_name))

    # create index on code column
    # make index unique for each table except carrier history
    session.execute('create {1} index {0}_code_idx ON {0} (code)'.format(
        table_name,
        '' if table_name == 'carrier_history' else 'unique'
    ))

    session.commit()
    

def main():

    # engine used by pandas (which use sqlalchemy) to connect to the database
    engine = create_engine("postgresql://postgres:testpass@localhost:5432/flights")

    # create a class used to create new session instances
    Session = sessionmaker(engine)

    # load csv file names to import
    csv_file_names = [
        (join(IMPORT_FOLDER, csv_file_name), re.match(
            "l_([a-zA-Z_0-9]*).csv", csv_file_name.lower()
        ).group(1))
        for csv_file_name in listdir(IMPORT_FOLDER)
        if isfile(join(IMPORT_FOLDER, csv_file_name))
    ]

    csv_file_names.sort()

    import_threads = []

    for csv_file_name, table_name in csv_file_names:

        # create a new thread for each lookup import
        # because of multiple threads do not run this in
        # Visual Studio Code console
        import_threads.append(ImportLookupThread(
            len(import_threads) + 1,
            table_name,
            csv_file_name,
            Session(),
        ))

    # start the threads
    for import_thread in import_threads:
        import_thread.start()

if __name__ == '__main__':
    main()