import pandas
import pickle
import logging
import pgdb
import os

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger("io_methods")

class IOMethods:
    def __init__(self, input_directory: str, output_directory: str, log_level):
        try:
            os.stat(input_directory)
            os.stat(output_directory)
            logger.debug('in/out directories exist')
        except:
            os.mkdir(input_directory)
            os.mkdir(output_directory)
            logger.debug('making directories')
        self.input_directory = input_directory
        self.output_directory = output_directory
        self.log_level = log_level

    @staticmethod
    def do_query(conn: pgdb.Connection, query: str):
        logger.info('connecting cursor to database %s' %conn)
        cur = conn.cursor()
        logger.info('executing query on database...')
        cur.execute(query)
        logger.info('returning query list')
        return cur.fetchall()

    def read_file_to_dataframe(self, file_name: str, file_delimiter: str) -> pandas.DataFrame:

        logger.info('reading ' + file_name + ' to pandas dataframe.')

        if self.input_directory[-1] == '/':
            file_path = self.input_directory + file_name
        else:
            file_path = self.input_directory + '/' + file_name

        try:
            df = pandas.read_table(file_path, delimiter=file_delimiter)
        except FileNotFoundError:
            logger.error('File not found at:\n'+file_path)

        logger.debug('df.info\n%s\ndf.tail(1)\n%s' % ( df.info(), df.tail(1) ) )

        return df

    def save_dataframe_2_csv(self, df: pandas.DataFrame, file_name: str):

        logger.info('writing pandas dataframe to csv file.')

        if self.output_directory[-1] == '/':
            file_path = self.output_directory + file_name
        else:
            file_path = self.output_directory + '/' + file_name

        df.to_csv(path_or_buf=file_path, index=False)

        logger.info('File written to location\n' + file_path)

    def save_dict_2_csv(self, dictionary: dict, file_name: str):
        import csv

        if self.output_directory[-1] == '/':
            file_path = self.output_directory + file_name
        else:
            file_path = self.output_directory + '/' + file_name

        with open(file_path, 'w') as f:  # Just use 'w' mode in 3.x
            w = csv.DictWriter(f, dictionary.keys())
            w.writeheader()
            w.writerow(dictionary)

        logger.info('File written to location\n' + file_path)

    def load_obj(self, file_name: str):

        logger.info('reading .pkl file.')

        if self.input_directory[-1] == '/':
            file_path = self.input_directory + file_name
        else:
            file_path = self.input_directory + '/' + file_name

        try:
            with open(file_path + '.pkl', 'rb') as f:
                return pickle.load(f)
        except FileNotFoundError:
            logger.error('File not found at:\n'+file_path)

    def save_obj(self, obj: object, file_name: str):

        logger.info('writing object to pkl binary file.')
        logger.debug('output_dir: %s' %self.output_directory)
        if self.output_directory[-1] == '/':
            file_path = self.output_directory + file_name
        else:
            file_path = self.output_directory + '/' + file_name

        with open(file_path + '.pkl', 'wb') as f:
            pickle.dump(obj, f, pickle.HIGHEST_PROTOCOL)

    def write_2_thread_time(self,execution_string: str, file_name: str):

        logger.info('writing to thread time file.')

        if self.output_directory[-1] == '/':
            file_path = self.output_directory + file_name
        else:
            file_path = self.output_directory + '/' + file_name
        try:
            t = open(file_path, 'a+')
            t.write(execution_string)
            t.close()
        except NotADirectoryError:
            logger.error('Not a directory error when writing thread_time file.')
        except UnboundLocalError:
            logger.error('execution time string not created when verbose = False')