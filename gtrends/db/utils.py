import mysql.connector
from mysql.connector import Error
import os
import re
import json
from dotenv import load_dotenv

load_dotenv('gtrends/db/.env')

def get_db_config(db='books'):
    DB_PORT = None
    if db =='books':
        DB_HOST = os.environ.get("DB_HOST")
        DB_USER = os.environ.get("DB_USER")
        DB_PASSWORD = os.environ.get("DB_PASSWORD")
        DB_NAME = os.environ.get("DB_NAME")
    elif db=='office_crm':
        DB_HOST = os.environ.get("CRM_DB_HOST")
        DB_USER = os.environ.get("CRM_DB_USER")
        DB_PASSWORD = os.environ.get("CRM_DB_PASSWORD")
        DB_NAME = os.environ.get("CRM_DB_NAME")
    elif db=='apps':
        DB_HOST = os.environ.get("APPDB_HOST")
        DB_USER = os.environ.get("APPDB_USERNAME")
        DB_PASSWORD = os.environ.get("APPDB_PASSWORD")
        DB_NAME = os.environ.get("APPDB_NAME")
        DB_PORT = os.environ.get("APPDB_PORT")

    # MySQL connection parameters
    db_config = {
        'host': DB_HOST, # replace with your actual host
        'database': DB_NAME, # replace with your actual database
        'user': DB_USER, # replace with your actual username
        'password': DB_PASSWORD, # replace with your actual password
    }
    if DB_PORT:
        db_config['port'] =  DB_PORT  # replace with your actual port, e.g., 3306]
    return db_config

def execute_queries_output_rows(queries, db='books'):
    connection = None  # Initialize connection variable
    rows_affected_per_query = []  # List to store the number of rows affected for each query

    try:
        # Establish a database connection
        db_config = get_db_config(db)
        connection = mysql.connector.connect(**db_config)
        if connection.is_connected():
            print("Successfully connected to the database")
        cursor = connection.cursor()

        # Execute each query in the list
        for query in queries:
            try:
                print(f"Executing query: {query}")
                cursor.execute(query)
                
                # Store the number of rows affected by the current query
                rows_affected_per_query.append(cursor.rowcount)
            except Error as e:
                print(f"Error executing query: {query}\nError: {e}")
                rows_affected_per_query.append(0)  # Append 0 if there is an error for this query

        # Commit the transaction
        try:
            connection.commit()
            print("All queries committed.")
        except Error as e:
            print(f"Error committing transaction: {e}")
            rows_affected_per_query.append(0)  # In case commit fails, log 0 for the whole batch

    except Error as e:
        print(f"Error connecting to the database: {e}")
    finally:
        # Close the connection
        if connection.is_connected():
            cursor.close()
            connection.close()
            print("Database connection closed.")

    # Print and return the number of rows affected for each query
    for i, query in enumerate(queries):
        print(f"Query: {query} | Rows affected: {rows_affected_per_query[i]}")
    
    return rows_affected_per_query

# Subroutine to execute the generated SQL queries
def execute_queries(queries, db='books'):
    connection = None  # Initialize connection variable
    try:
        db_config = get_db_config(db)
        connection = mysql.connector.connect(**db_config)
        if connection.is_connected():
            print("Successfully connected to the database")
        cursor = connection.cursor()

        # Execute each query in the list
        for query in queries:
            try:
                print(f"Executing query: {query}")
                cursor.execute(query)
            except Error as e:
                print(f"Error executing query: {query}\nError: {e}")
                error_message = str(e)
                print(f"An error occurred: {error_message}")

        # Commit the transaction
        try:
            connection.commit()
            print("All queries committed.")
        except Error as e:
            print(f"Error committing transaction: {e}")
    except Error as e:
        print(f"Error connecting to the database: {e}")
    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()
            print("Database connection closed.")

def execute_select_query(query, db='books'):
    connection = None  # Initialize connection variable
    result = []  # To store the result as a list of dictionaries
    try:
        db_config = get_db_config(db)
        connection = mysql.connector.connect(**db_config)
        if connection.is_connected():
            print("Successfully connected to the database")
        cursor = connection.cursor(dictionary=True)  # Use dictionary cursor to get results as dictionaries

        print(f"Executing query: {query}")
        cursor.execute(query)
        
        # Fetch all rows
        rows = cursor.fetchall()
        result = [dict(row) for row in rows]  # Convert each row to a dictionary

    except Error as e:
        print(f"Error executing query: {query}\nError: {e}")
    finally:
        if connection and connection.is_connected():
            cursor.close()
            connection.close()
            print("Database connection closed.")

    return result

def execute_insert_queries(insert_query, data_values, db='books', try_each = False):
    connection = None  # Initialize connection variable
    failed_data = []  # List to store failed data
    try:
        # Establish database connection
        db_config = get_db_config(db)
        connection = mysql.connector.connect(**db_config)
        if connection.is_connected():
            print("Successfully connected to the database")
        cursor = connection.cursor(dictionary=True)  # Use dictionary cursor to get results as dictionaries

        # Execute each query individually if try_each is True
        if try_each:
            for data in data_values:
                try:
                    # Execute the insert query
                    cursor.execute(insert_query, data)
                    # Commit each insert query execution
                    connection.commit()
                except Error as e:
                    # If there's an error, print it and add the data to failed_data
                    print(f"Error executing query with data: {data}\nError: {e}")
                    failed_data.append(data)  # Store the failed data

        # Otherwise, execute all queries together
        else:
            for data in data_values:
                cursor.execute(insert_query, data)  # Execute all queries
            connection.commit()  # Commit the batch execution

    except Error as e:
        print(f"Error executing query: {insert_query}\nError: {e}")
    
    finally:
        if connection and connection.is_connected():
            cursor.close()
            connection.close()
            print("Database connection closed.")
    
    return failed_data if try_each else True

def execute_query(query, db='books'):
    connection = None  # Initialize connection variable
    try:
        # Establish database connection
        db_config = get_db_config(db)
        connection = mysql.connector.connect(**db_config)
        if connection.is_connected():
            print("Successfully connected to the database")
        cursor = connection.cursor(dictionary=True)  # Use dictionary cursor to get results as dictionaries

        print(f"Executing query: {query}")
        cursor.execute(query)
        # Commit query execution
        connection.commit()

    except Error as e:
        print(f"Error executing query: {query}\nError: {e}")
    finally:
        if connection and connection.is_connected():
            cursor.close()
            connection.close()
            print("Database connection closed.")
    
    return True

def execute_update_queries(queries, db='books'):
    connection = None  # Initialize connection variable
    failed_queries = []
    try:
        # Establish database connection
        db_config = get_db_config(db)
        connection = mysql.connector.connect(**db_config)
        if connection.is_connected():
            print("Successfully connected to the database")
        cursor = connection.cursor(dictionary=True)  # Use dictionary cursor to get results as dictionaries
        for query in queries:
            print(f"Executing query: {query}")
            cursor.execute(query)
            # Commit query execution
            connection.commit()

    except Error as e:
        print(f"Error executing query: {query}\nError: {e}")
        failed_queries.append(query)
    finally:
        if connection and connection.is_connected():
            cursor.close()
            connection.close()
            print("Database connection closed.")
    
    return {"status" : True, "successful" : len(queries) - len(failed_queries), "failed" : failed_queries}

def execute_insert_queries_get_ids(insert_query, data_values, db='books', key_map = []):
    connection = None  # Initialize connection variable
    inserted_ids = {}  # List to store inserted row IDs
    try:
        # Establish database connection
        db_config = get_db_config(db)
        connection = mysql.connector.connect(**db_config)
        if connection.is_connected():
            print("Successfully connected to the database")
        cursor = connection.cursor(dictionary=True)  # Use dictionary cursor to get results as dictionaries

        # Loop through the data and execute each query
        if not key_map:
            key_map = range(len(data_values))
            
        for uid, data in zip(key_map, data_values):
            cursor.execute(insert_query, data)

            # Get the ID of the last inserted row
            inserted_id = cursor.lastrowid
            inserted_ids.update({uid : inserted_id})
            
        # Commit insert queries execution
        connection.commit()

    except Error as e:
        print(f"Error executing query: {insert_query}\nError: {e}")
    finally:
        if connection and connection.is_connected():
            cursor.close()
            connection.close()
            print("Database connection closed.")
    
    return inserted_ids  # Return the dict of inserted row IDs

def execute_data_queries(data_query, data_values, db='books'):
    connection = None
    cursor = None
    success = 0
    try:
        db_config = get_db_config(db)
        connection = mysql.connector.connect(**db_config)
        if connection.is_connected():
            print("Successfully connected to the database")
        cursor = connection.cursor(dictionary=True)

        for data in data_values:
            cursor.execute(data_query, data)
            connection.commit()
            success += 1

    except mysql.connector.Error as e:
        print(f"Error executing query: {data_query}\nError: {e}")

    finally:
        if cursor:
            cursor.close()
        if connection and connection.is_connected():
            connection.close()
            print("Database connection closed.")
    
    return {"status" : True, "successful" : success, "failed" : len(data_values) - success}