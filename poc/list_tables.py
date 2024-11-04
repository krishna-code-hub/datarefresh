import sqlite3


def list_all_tables(database_path):
    try:
        # Connect to the database
        connection = sqlite3.connect(database_path)
        cursor = connection.cursor()

        # Query to list all tables
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = cursor.fetchall()

        # Print the list of tables
        if tables:
            print("Available tables:")
            for table in tables:
                print(table[0])
        else:
            print("No tables found in the database.")
    except sqlite3.Error as e:
        print(f"Error: {e}")
    finally:
        if connection:
            connection.close()


# Example usage
list_all_tables("../data/customer_data.db")
