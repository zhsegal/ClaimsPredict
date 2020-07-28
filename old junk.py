def create_merged_sql(self, df1, df2, table_name):
    with sqlite3.connect(
            f'DB/{table_name}.db') as conn:  # You can create a new database by changing the name within the quotes
        cursor = conn.cursor()  # The database will be saved in the location where your 'py' file is saved

        df = pd.concat([df1, df2])
        df.to_sql(table_name, conn, if_exists='append', index=False)

    print(f'uploaded {table_name} to db')



def run_multiprocess(self, patient_ids):
        results_df = pd.DataFrame()
        pool=mp.Pool(processes=self.config['multiprocessing']['process_number'])

        results=[]

        batch_number=len(range(0, len(patient_ids), self.batch_size))
        with tqdm(total=batch_number) as pbar:
            for ids in (self.chunkizer(patient_ids,self.batch_size)):
                result=pool.apply_async(func=self.calculate_batch, args=(ids,))
                pbar.update()
                results.append(result)


        pool.close()
        pool.join()
        for result in results: results_df = results_df.append(result.get())
        return results_df