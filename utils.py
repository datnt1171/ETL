def insert_data(df_data, table_name, conn):
    """
    Inserts data from a DataFrame into a specified PostgreSQL table.

    Parameters:
    df_data (pd.DataFrame): DataFrame containing the data to insert.
    conn (psycopg2 connection): Active connection to the PostgreSQL database.
    table_name (str): The name of the table to insert data into.
    """
    try:
        # Get the column names from the DataFrame
        columns = list(df_data.columns)

        # Create the SQL insert query dynamically based on column names
        insert_query = f"""
            INSERT INTO {table_name} ({', '.join(columns)})
            VALUES ({', '.join(['%s'] * len(columns))});
        """

        # Convert the DataFrame to a list of tuples (for batch insert)
        data = [tuple(row[1:]) for row in df_data.itertuples()]

        # Execute the batch insert
        cur = conn.cursor()
        cur.executemany(insert_query, data)
        conn.commit()

        print("Data inserted successfully!")
    except Exception as e:
        conn.rollback()  # Rollback in case of any error
        print(f"An error occurred: {e}")