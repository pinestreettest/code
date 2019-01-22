import numpy as np
import pandas as pd
import pandas_redshift as pr
import psycopg2
import datetime

from io import StringIO
from configparser import ConfigParser
import constants

"""
OVERVIEW: A pandas version of what Zach has put together for credit recog.,
            torwards a scalable, deployable code base

STATUS: WIP
    - Infastructure is good to go

NOTE: Read_sql does not refresh the db so the call will not recognize any
        table that has been written to db in script withing a read call
"""

class ParamParser:
    def __init__(self, config_name, section):
        self.config_name = config_name
        self.section = section
        self.parameters = self.parse_parameters(self.config_name, self.section)

    def parse_parameters(self, config_name, section):
        try:
            parser = ConfigParser()
            parser.read(config_name)

            parameters = {}
            if parser.has_section(section):
                params = parser.items(section)
                for param in params:
                    parameters[param[0]] = param[1]
                return parameters
            else:
                raise Exception('Section {0} not found in the {1} file'.format(section, config_name))
        except (Exception):
            print('Config arguments not set properally...')

# Sets up connection to db defined within the config file
class Builder:
    def __init__(self, config_name, db_name):
        self.config_name = config_name
        self.db_name = db_name
        self.db_conn=self.db_connection(self.config_name, self.db_name)

    def db_connection(self, filename, section):
        try:
            db_params = ParamParser(filename, section)
            print('\nConnecting to db...')
            conn = psycopg2.connect(**db_params.parameters)
            print ('Connection made\n')
            return conn
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)

    def read_table_to_df(self, db_conn, schema, filename):
        sql = """select * from {schema}.{filename}""".format(schema=schema, filename=filename)
        #sql = """select * from {schema}.{filename} limit 100""".format(schema=schema, filename=filename)
        print('Reading in {schema}.{filename}...'.format(schema=schema, filename=filename))
        df = pd.read_sql(sql, db_conn)
        print('{schema}.{filename} read in to dataframe.\n'.format(schema=schema, filename=filename))
        return df

    def write_df_to_db(self, data_frame, schema, table_name):

        s3_params = ParamParser(self.config_name, constants.bucket_name)
        pr.connect_to_s3(**s3_params.parameters)

        redshift_params = ParamParser(self.config_name, constants.db_name)
        pr.connect_to_redshift(**redshift_params.parameters)

        col_types = []
        for i, j in zip(data_frame.columns, data_frame.dtypes):
            if "object" in str(j):
                try:
                    col_types.append('varchar({})'.format(int(data_frame[i].str.len().max() + 10)))
                except:
                    col_types.append('TEXT')
            elif "datetime" in str(j):
                col_types.append('TIMESTAMP')
            elif "float" in str(j):
                col_types.append('FLOAT')
            elif "int64" in str(j):
                col_types.append('INT8')
            elif "int" in str(j):
                col_types.append('INT')
            else:
                col_types.append('TEXT')
        print("Begin writing {schema}.{table_name} to db...".format(schema=schema, table_name=table_name))
        pr.pandas_to_redshift(data_frame=data_frame, redshift_table_name=schema + '.' + table_name, column_data_types=col_types)
        print("Writing complete.\n")

    def close_connection(self, conn):
        conn.close()
        print("db connection is closed")

    def ping_db(self, conn):
        temp = pd.read_sql("""select * from public.user limit 1""", db_handler.db_conn)
        print("First row of of public.user table as dataframe...")
        print(temp)

if __name__ == "__main__":
    # Add arrgs
    # args = inputparser.parse_args()

    # To be parameters!!!
    year1 = 2018
    month1 = 11
    day1 = 30
    year2 = 2018
    month2 = 12
    day2 = 1

    db_handler = Builder(constants.config_name, constants.db_name)

    # Read in class function is for developement only, will replace with more specific sql calls
    #  so that less memory is used....
    # More of an exercise in learning the data right now

    """
    # *** First Function Set -> z_brwr1 ***
    # Function 1 working
    user_df = db_handler.read_table_to_df(db_handler.db_conn, constants.public_schema, constants.user_table)
    brwr_df = db_handler.read_table_to_df(db_handler.db_conn, constants.public_schema, constants.brwr_table)
    user_df.rename(columns={'id':'user_id'}, inplace=True)
    z_brwr1 = brwr_df.merge(user_df, on='user_id', how='left').rename(columns={'id':'borrower_id'}).reset_index(drop=True)
    z_brwr1 = z_brwr1[['borrower_id', 'user_id', 'account_id', 'email']].drop_duplicates().reset_index(drop=True)

    # Funtion 2 and 3 working
    daily_brwr_df_1 = db_handler.read_table_to_df(db_handler.db_conn, constants.analytcs_schema, constants.daily_brwr_table + str(year1) + str(month1))
    daily_brwr_df_2 = db_handler.read_table_to_df(db_handler.db_conn, constants.analytcs_schema, constants.daily_brwr_table+ str(year2) + str(month2))
    z_bal_s = daily_brwr_df_1[daily_brwr_df_1['curr_date'] == datetime.date(year1, month1, day1)]
    z_bal_s = z_bal_s[['borrower_id', 'balances_repayment', 'balances_bad']]
    z_bal_e = daily_brwr_df_2[daily_brwr_df_2['curr_date'] == datetime.date(year2, month2, day2)]
    z_bal_e = z_bal_e[['borrower_id', 'balances_repayment', 'balances_bad']]

    # Funtion 4 working
    brwr_bal = db_handler.read_table_to_df(db_handler.db_conn, constants.analytcs_schema, constants.brwr_bal_table + str(year2) + str(month2))
    z_brwr1_brwr_id = list(z_brwr1['borrower_id'].unique())
    z_credits = brwr_bal[brwr_bal['borrower_id'].isin(z_brwr1_brwr_id)]
    z_credits = z_credits[z_credits['curr_date'] == datetime.date(year2, month2, day2)]
    z_credits = z_credits[['borrower_id', 'bal_s', 'bal_e']]

    # Function 5 working
    z_brwr1 = z_brwr1.merge(z_bal_s, on='borrower_id', how='left').reset_index(drop=True).rename(columns={'balances_repayment':'balances_repayment_s', 'balances_bad':'balances_bad_s'})
    z_brwr1 = z_brwr1.merge(z_bal_e, on='borrower_id', how='left').reset_index(drop=True).rename(columns={'balances_repayment':'balances_repayment_e', 'balances_bad':'balances_bad_e'})
    z_brwr1 = z_brwr1.merge(z_credits, on='borrower_id', how='left').reset_index(drop=True).rename(columns={'bal_s':'balances_credit_s', 'bal_e':'balances_credit_e'})
    z_brwr1['curr_date'] = datetime.date(year2, month2, day2)

    # TEMP - Save to db for faster testing...
    db_handler.write_df_to_db(z_brwr1, constants.dev_schema, constants.name_z_brwr1)
    """
    # TEMP - Read from db for faster testing...
    z_brwr1 = pd.read_sql("""select * from test_perpay_analytics.dev_z_brwr1""", db_handler.db_conn)
    #z_brwr1 = db_handler.read_table_to_df(db_handler.db_conn, constants.dev_schema, constants.name_z_brwr1)

    # *** Second Function Set ***
    # Function 6 working
    z_brwr = db_handler.read_table_to_df(db_handler.db_conn, constants.public_schema, constants.core_acct_bal_table)
    z_brwr = z_brwr[['user_id', 'created', 'starting_balance', 'ending_balance', 'id']]
    z_brwr = z_brwr[(z_brwr['created'] >= pd.Timestamp(datetime.date(year2, month2, day2))) &
                    (z_brwr['created'] < pd.Timestamp(datetime.date(year2, month2, day2+1)))]

    # Function 7, 8, 9 working
    z_brwr['rank_start'] = z_brwr.sort_values(['id'], ascending=[True]).groupby('user_id').cumcount()
    z_brwr['rank_end'] = z_brwr.sort_values(['id'], ascending=[False]).groupby('user_id').cumcount()
    z_brwr['bal_s'] = z_brwr.loc[z_brwr.groupby('user_id')['rank_start'].idxmin(), 'starting_balance']
    z_brwr['bal_e'] = z_brwr.loc[z_brwr.groupby('user_id')['rank_end'].idxmin(), 'ending_balance']
    z_brwr = z_brwr.groupby('user_id')[['bal_s', 'bal_e']].first().reset_index()

    # Last fcn working
    z_brwr1 = z_brwr1.merge(z_brwr, on='user_id', how='left').reset_index(drop=True)



    db_handler.close_connection(db_handler.db_conn)







    # hold spaces
