import psycopg2
import pandas as pd


class PostgresDBUtils:
    """
    A class used to connect and communicated with our postgres database

    ...

    Methods
    -------
    create_db(db_name)
        creates database
    create_tables(table_name)
        creates table
    insert_to_table(df, table_name)
        inserts data into a table
    db_execute_fetch(table_name)
        fetches data from a table
    close_connection
        closes the database connection
    """

    def __init__(self):
        """
        Attributes
        ----------
        conn : database connection
            The database connection
        cursor : cursor
            The cursor object
        """

        try:
            # establishing the connection
            self.conn = psycopg2.connect(
                database="adludio_data_science_challenge", user='airflow', password='airflow', host='127.0.0.1', port='5432'
            )
            self.conn.autocommit = True

            # creating a cursor object using the cursor() method
            self.cursor = self.conn.cursor()
        except Exception as e:
            pass

    def create_db(self, db_name: str) -> None:
        """
        Creates a database with the specified database name.

        Parameters
        ----------
        db_name : str
            The name of the database we are creating
        """

        # Preparing query to create a database
        sql = f'''CREATE database ${db_name}'''
        # Creating a database
        self.cursor.execute(sql)

    def create_tables(self, table_name: str) -> None:
        """
        Creates a table with the specified table name.

        Parameters
        ----------
        table_name : str
            The name of the table we are creating
        """

        # dropping table if already exists.
        self.cursor.execute(f"DROP TABLE IF EXISTS ${table_name}")

        sql_file = 'schema.sql'
        fd = open(sql_file, 'r')
        read_sql_file = fd.read()
        fd.close()

        sql_commands = read_sql_file.split(';')

        for command in sql_commands:
            try:
                self.cursor.execute(command)
            except Exception as ex:
                print("Command skipped: ", command)
                print(ex)
        self.conn.commit()
        self.cursor.close()

    def insert_to_table(self, df: pd.DataFrame, table_name: str) -> None:
        """
        Inserts dataframe to a table with the specified name.

        Parameters
        ----------
        table_name : str
            The name of the table we are inserting into
        df: pd.Dataframe
            The dataframe we are inserting

        """

        for _, row in df.iterrows():
            sql_query = f"""INSERT INTO {table_name} (campaign_id, campaign_name, submission_date, description, 
                campaign_objectives, kpis, placements, start_date, end_date, serving_locations, 
                black_white_audience_list_included, delivery_requirements, cost_centre, currency, buy_rate, 
                volume_agreed, gross_cost, agency_fee, percentage, flat_fee, net_cost, type, width, height, creative_id,
                auction_id, browser_ts, game_key, geo_country, site_name, platform_os, device_type, browser, labels, 
                text, colors, video_data, eng_type, direction, adunit_size,)
                     VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 
                     %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);"""
            data = (
                row[0], row[1], row[2], row[3], (row[4]), (row[5]), row[6], row[7], row[8], row[9], row[10], row[11],
                row[12], row[13], row[14], row[15], row[16], row[17], row[18], row[19], row[20], row[21], row[22],
                row[23], row[24], row[25], (row[26]), (row[27]), row[28], row[29], row[30], row[31], row[32], row[33],
                row[34], row[35], row[36], row[37], row[38], row[39])

            try:
                # Execute the SQL command
                self.cursor.execute(sql_query, data)

                # Commit your changes in the database
                self.conn.commit()
                print("Data Inserted Successfully")
            except Exception as e:
                self.conn.rollback()
                print("Error: ", e)
        return

    def db_execute_fetch(self, table_name='', return_df=True) -> pd.DataFrame:
        """
        Fetches dataframe from a table with the specified name.

        Parameters
        ----------
        table_name : str
            The name of the table we are inserting into
        return_df: bool
            Return dataframe or list

        """

        query = f""" select * from {table_name}"""
        # if many:
        #     self.cursor.executemany(*args)
        # else:
        #     self.cursor.execute(*args)
        self.cursor.execute(query)
        # get column names
        field_names = [i[0] for i in self.cursor.description]

        # get column values
        res = self.cursor.fetchall()

        # get row count and show info
        nrow = self.cursor.rowcount
        if table_name:
            print(f"{nrow} records fetched from {table_name} table")

        self.cursor.close()
        self.conn.close()

        if return_df:
            return pd.DataFrame(res, columns=field_names)
        else:
            return res

    def close_connection(self):
        """
        Close the connection with the database.

        """
        self.conn.close()


if __name__ == "__main__":
    pgu = PostgresDBUtils()
    pgu.db_execute_fetch(table_name='ads_data_joined')
