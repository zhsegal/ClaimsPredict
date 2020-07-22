def create_merged_sql(self, df1, df2, table_name):
    with sqlite3.connect(
            f'DB/{table_name}.db') as conn:  # You can create a new database by changing the name within the quotes
        cursor = conn.cursor()  # The database will be saved in the location where your 'py' file is saved

        df = pd.concat([df1, df2])
        df.to_sql(table_name, conn, if_exists='append', index=False)

    print(f'uploaded {table_name} to db')