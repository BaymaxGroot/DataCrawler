import os
import time
import pyodbc
from crawlers.common.helpers import set_environment_config

set_environment_config('config.yml')


def db_conn():
    """
    create a connection object
    :return:
    """
    try:
        db_address_list = os.environ['DB_SERVER'].split(":")
        db_address = "tcp:" + db_address_list[0] + "," + db_address_list[1]
        db_user = os.environ['DB_USER']
        db_password = "{" + os.environ['DB_PASSWORD'] + "}"
        db_database = os.environ['DB_DATABASE']

        conn = pyodbc.connect('DRIVER={};SERVER={};DATABASE={};UID={};PWD={};'.format(
            os.environ['DB_DRIVER'],
            db_address,
            db_database,
            db_user,
            db_password))
        return conn
    except Exception as e:
        print("Failed to create a connection to connect the database")
        raise


def db_query(query_sql, single=False):
    """
    query database through given sql and return a list of dict as query result
    :param query_sql:
    :param single:
    :return:
    """
    cnxn = None
    retry = 0
    while retry < 3:
        try:
            cnxn = db_conn()
            cursor = cnxn.cursor()
            cursor.execute(query_sql)
            results = [dict((cursor.description[i][0], value) for i, value in enumerate(row)) for row in
                       cursor.fetchall()]

            to_return = results

            if single:
                to_return = results[0] if results else None
            return to_return
        except pyodbc.Error:
            print("Failed to query database! waiting for retry...")
            retry += 1
            time.sleep(5)
        except Exception as e:
            print("Failed to query database! the sql is: " + query_sql)
            retry += 1
            raise
        finally:
            if cnxn:
                cnxn.close()


def insert_update_delete(sql):
    """
    execute one single sql for insert or update or delete
    :param sql:
    :return:
    """
    cnxn = None
    retry = 0
    while retry < 3:
        try:
            cnxn = db_conn()
            cursor = cnxn.cursor()
            cursor.execute(sql)
            cnxn.commit()
            break
        except pyodbc.Error:
            print("Failed to update the database! Waiting for retry...")
            if cnxn:
                cnxn.rollback()
            retry += 1
            time.sleep(5)
        except Exception as e:
            print("Error to execute sql: {}. Exception: {}".format(sql, str(e)))
            if cnxn:
                cnxn.rollback()
            retry += 1
            raise
        finally:
            if cnxn:
                cnxn.close()


def batch_insert_update_delete(sql_list):
    """
    batch insert or update or delete sql lists
    :param sql_list:
    :return:
    """
    cnxn = None
    retry = 0
    sql_command = None
    while retry < 3:
        try:
            cnxn = db_conn()
            cursor = cnxn.cursor()

            for sql in sql_list:
                sql_command = sql
                cursor.execute(sql)

            cnxn.commit()
            break
        except pyodbc.Error:
            print("Failed to update the database! Waiting for retry...")
            print("Execute the command is {}".format(sql_command))
            if cnxn:
                cnxn.rollback()
            retry += 1
            time.sleep(5)
        except Exception as e:
            print("Error to execute sql list! Exceptiopn: {}".format(str(e)))
            if cnxn:
                cnxn.rollback()
            retry += 1
            raise
        finally:
            if cnxn:
                cnxn.close()
