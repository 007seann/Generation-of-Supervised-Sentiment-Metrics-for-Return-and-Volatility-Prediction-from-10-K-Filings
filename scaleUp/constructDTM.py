from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit
from pyspark.sql.types import StructType, StructField, StringType
import sys
import os
# Add the parent directory to sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
# Import the function
import pandas as pd
import redis


class ConstructDTM:
    def __init__(self, spark, data_folder, save_folder, firms_dict, firms_ciks, columns, start_date, end_date):
        self.spark = spark
        self.data_folder = data_folder
        self.save_folder = save_folder
        self.firms_dict = firms_dict
        self.firms_ciks = firms_ciks
        self.columns = columns
        self.start_date = start_date
        self.end_date = end_date
        self.output_folder = os.path.join(save_folder, 'company_df')
        os.makedirs(self.output_folder, exist_ok=True)
        
        # Add hons_project.zip to SparkContext
        self.spark.sparkContext.addPyFile("/Users/apple/PROJECT/package/hons_project.zip")
        
        # Initialise Redis connection
        self.redis_client = redis.Redis(host='localhost', port=6379, db=0)
        
    def load_cache(self, cik):
        """
        Load the set of already processed file names for a given CIK from Redis.
        Return a Python set of file names.
        """
        redis_key = f"processed_files:{cik}"
        processed_files = self.redis_client.smembers(redis_key)  # returns a set of bytes
        # Decode bytes to strings
        return {file_name.decode('utf-8') for file_name in processed_files}
    
    def save_cache(self, cik, file_names):
        """
        Add the given file names to the Redis set for a given CIK.
        """
        redis_key = f"processed_files:{cik}"
        for fn in file_names:
            self.redis_client.sadd(redis_key, fn)
            
    def detect_new_files(self, cik, directory_path):
        """
        Compare all files in the directory_path against the Redis cache for this CIK.
        Return a list of the full paths of files that are new.
        """
        # Load the already processed file names from Redis
        processed_files_cache = self.load_cache(cik)

        # Gather all files in the local directory
        all_files = [
            os.path.join(directory_path, f)
            for f in os.listdir(directory_path)
            if os.path.isfile(os.path.join(directory_path, f))
        ]

        # Determine which files are new (not in the processed_files_cache)
        new_file_paths = []
        for file_path in all_files:
            file_name = os.path.basename(file_path)
            if file_name not in processed_files_cache:
                new_file_paths.append(file_path)

        return new_file_paths

    def csv_builder(self):
        """
        Build CSV files for each CIK using Spark. Write a new CSV for new files only.
        """
        def process_cik(cik, symbol):
            cik_path = os.path.join(self.data_folder, cik)
            if not os.path.exists(cik_path):
                print(f"[csv_builder] Path does not exist for CIK: {cik}")
                return None
            
            # Detect new files using Redis-based cache
            new_files = self.detect_new_files(cik, cik_path)
            if not new_files:
                print(f"[csv_builder] No new files found for CIK: {cik}")
                return None
            
            # Parse ONLY the new files
            new_data = []
            for file_path in new_files:
                file_name = os.path.basename(file_path)
                date = file_name.split('.')[0]
                body = self.import_file(file_path)  # Replace with appropriate file reading logic
                new_data.append((symbol, cik, date, body))
                
            # Create a Spark DataFrame with the new data
            new_df = self.spark.createDataFrame(new_data, schema=self.columns)
            new_df = new_df.dropna(how="all", subset=new_df.columns)
            new_df = new_df.select(["Name", "CIK", "Date", "Body"])
            
            # Sort by date 
            new_df = new_df.orderBy(col("Date"))
            
            # Write the new files to a new CSV path
            output_path = os.path.join(self.output_folder, cik)
            new_df.coalesce(1).write.csv(output_path, header=True, mode="append") # Dynamic batch = a set of new files. i.e., a batch size is changed corresponding to a number of new files uploaded to the folder.
            
            # Update the Redis cache with these new file names
            new_file_names = [os.path.basename(f) for f in new_files]
            self.save_cache(cik, new_file_names)
            
            print(f"[csv_builder] Wrote new CSV for {len(new_files)} file(s) under CIK: {cik}.")


        for cik, symbol in self.firms_dict.items():
            output_path = os.path.join(self.output_folder, cik)
            # if os.path.exists(output_path):
            #     print(f"CSV file already exists for CIK: {cik}, skipping...") # Not for real-time update 
            #     continue

            print(f"Processing CIK: {cik}")
            cik_df = process_cik(cik, symbol)
            if cik_df:
                # Save the DataFrame as a CSV file, sorted by date
                cik_df = cik_df.orderBy(col("Date"))
                cik_df.write.csv(output_path, header=True, mode="overwrite")

        print(f"[csv_builder] CSV files saved/updated in: {self.output_folder}")
        
    def aggregate_data(self, files_path, firms_ciks):
        
        folder = 'company_df'
        folder_path = os.path.join(files_path, folder)
        if not os.path.exists(folder_path):
            os.makedirs(folder_path, exist_ok=True)
        
        csv_files = []
        for cik in firms_ciks:
            cik_folder = os.path.join(folder_path, cik)
            if not os.path.exists(cik_folder):
                print(f"No folder found for CIK: {cik}")
                continue
            files = [f for f in os.listdir(cik_folder) if f.endswith('.csv')]
            if not files:
                print(f"No CSV files found for CIK: {cik}")
                continue
            for file in tqdm(files):
                if file.endswith('.csv'):
                    csv_files.append(pd.read_csv(os.path.join(folder_path, file)))# Read all CSV files in the folder
        
        # Concatenate all reports into a single DataFrame
        csv_files = pd.concat(csv_files)
        # reset index
        csv_files.reset_index(drop=True, inplace=True)
        # Save the DataFrame to a CSV file
        csv_files.to_csv(os.path.join(files_path, "SP500.csv"), index=False)

    def process_filings_for_cik_spark(self, spark, data_folder, files_path, firms_dict, firms_ciks, columns, start_date, end_date):
        """
        Process CIK filings using Spark in a distributed manner, applying the reader function to each file.
        Output : dtm_{cik}.csv
        """
        folder = 'company_df'
        folder_path = os.path.join(files_path, folder)
        if not os.path.exists(folder_path):
            os.makedirs(folder_path, exist_ok=True)
            
        
        def process_single_cik(cik):
            import os
            import pandas as pd
            from hons_project.vol_reader_fun import vol_reader
            from hons_project.annual_report_reader import reader
            
            
            # Define the output path for the processed CSV file
            output_path = os.path.join(files_path, f"processed/dtm_{cik}.csv")

            # Check if the output file already exists
            # Not for real-time update 
            # Need to be changed to update the dtm_{cik}.csv file
            # Refer to def append_to_dtm code in the Notion
            if os.path.exists(output_path):
                print(f"CSV file already exists for CIK: {cik}, skipping...")
                return None

            cik_folder = os.path.join(folder_path, cik)
            if not os.path.exists(cik_folder):
                print(f"No folder found for CIK: {cik}")
                return None

            # Combine all processed files for the CIK into a single DataFrame
            processed_data = []
            for file_name in os.listdir(cik_folder):
                if not file_name.endswith('.csv'):
                    continue
                # Apply the reader function
                processed_file = reader(file_name, file_loc=cik_folder)
                # processed_file.index = processed_file.index.tz_localize("UTC")

                if processed_file is not None:
                    processed_data.append(processed_file)
            if not processed_data:
                print(f"No valid files processed for CIK: {cik}")
                return None

            # Combine all processed files into a DataFrames
            combined_data = pd.concat(processed_data)
            # Convert the processed data to a Spark DataFrame
            # Read and join with volatility data

            vol_data = vol_reader(cik, start_date=start_date, end_date=end_date)

            
            # Merge the data
            combined_data = pd.merge(combined_data, vol_data, how="inner", on="Date")

            # Add the '_cik' column
            combined_data["_cik"] = cik

            # Convert the index to a column
            combined_data = combined_data.reset_index()

            # Reorder columns
            columns_to_move = ['Date', '_cik', '_vol', '_ret', '_vol+1', '_ret+1']
            new_column_order = columns_to_move + [col for col in combined_data.columns if col not in columns_to_move]
            combined = combined_data[new_column_order]

            # Filter rows with missing volatility data
            combined_data = combined_data[combined_data["_ret"].notnull()]

            # Save the processed DataFrame to a file
            save_folder = os.path.join(files_path, "processed")
            if not os.path.exists(save_folder):
                os.makedirs(save_folder)
            
            combined.to_csv(output_path, index=False)
            
            print(f"Processed CIK {cik} and saved to {output_path}")
            
        # Parallel processing of all CIKs
        cik_rdd = spark.sparkContext.parallelize(firms_ciks)
        cik_rdd.foreach(lambda cik: process_single_cik(cik))

    def concatenate_dataframes(self, level, section, save_path, start_date, end_date):
        from hons_project.vol_reader_fun import vol_reader2
        """
        Concatenate all processed DataFrames into a single DataFrame.
        """
        dataframes = []
        files_path = os.path.join(save_path, 'processed')
        for cik in self.firms_ciks:
            file_path = os.path.join(files_path, f'dtm_{cik}.csv')
            if os.path.exists(file_path):
                df = self.spark.read.csv(file_path, header=True, inferSchema=True)
                dataframes.append(df)
            else:
                print(f"File does not exist for CIK: {cik}, skipping...")

        if dataframes:
            # Combine all DataFrames
            combined_df = dataframes[0]
            for df in dataframes[1:]:
                combined_df = combined_df.unionByName(df, allowMissingColumns=True)

            combined_df = combined_df.fillna(0.0)
            combined_df = combined_df.withColumnRenamed('Date0', 'Date')
            combined_df = combined_df.orderBy("Date")

            if not os.path.exists(save_path):
                os.makedirs(save_path)

            # Align with 3-day return/volatility
            x1, x2 = vol_reader2(self.firms_ciks, start_date, end_date, window=3, extra_end=True, extra_start=True)
            x1 = x1.shift(1)
            x2 = x2.shift(1)
            x1 = x1[start_date:end_date]
            x2 = x2[start_date:end_date]


            first = True
            for cik in self.firms_ciks:
                print(f'Processing {cik}')
                x1c = x1[cik]
                x2c = x2[cik]
                x = pd.concat([x1c, x2c], axis=1)
                x.columns = ['n_ret', 'n_vol']
                y = combined_df.filter(combined_df['_cik'] == int(cik.lstrip('0')))

                y = y.withColumn('Date', y['Date'].cast('timestamp'))
                x.index = pd.to_datetime(x.index)
                y = y.toPandas()
                y.set_index('Date', inplace=True)
                z = y.join(x)
                zz = z[['n_ret', 'n_vol']]

                if first:
                    df_add = zz
                    first = False
                else:
                    df_add = pd.concat([df_add, zz], axis=0)

            df_add.reset_index(inplace=True)
            assert all(combined_df.toPandas().index == df_add.index), 'Do not merge!'

            combined_df = combined_df.toPandas()
            combined_df['_ret'] = df_add['n_ret']
            combined_df['_vol'] = df_add['n_vol']


            filename = f'dtm_{level}_{section}'
            file_path = os.path.join(save_path, f"{filename}.csv")
            combined_df.to_csv(file_path, index=False)
        else:
            print("No dataframes to concatenate.")

    @staticmethod
    def import_file(file_path):
        """
        Placeholder function for importing file content.
        Replace with the actual file reading logic.
        """
        with open(file_path, 'r', encoding='latin-1') as file:
            return file.read()


# Example Usage
if __name__ == "__main__":
    # Initialize Spark session
    spark = SparkSession.builder \
        .appName("Integrated Data Pipeline") \
        .getOrCreate()

    # Define input parameters
    # data_folder = "/Users/apple/PROJECT/Code_4_SECfilings/total_sp500_10q-txt"
    data_folder = "/Users/apple/PROJECT/Code_4_SECfilings/test.filings"
    save_folder = "/Users/apple/PROJECT/hons_project/data/SP500"
    firms_csv_file_path = "/Users/apple/PROJECT/Code_4_SECfilings/test_constituents.csv"
    firms_df = pd.read_csv(firms_csv_file_path)
    firms_df['CIK'] = firms_df['CIK'].apply(lambda x: str(x).zfill(10))
    firms_dict = firms_df.set_index('Symbol')['CIK'].to_dict()
    firms_dict = {value: key for key, value in firms_dict.items()}

    
    firms_ciks = list(firms_dict.keys())
    columns = ["Name", "CIK", "Date", "Body"]
    start_date = '2006-01-01'
    end_date = '2023-12-31'

    # Create pipeline and execute tasks
    pipeline = ConstructDTM(spark, data_folder, save_folder, firms_dict, firms_ciks, columns, start_date, end_date)
    pipeline.csv_builder()
    # pipeline.process_filings_for_cik_spark(spark, data_folder, save_folder, firms_dict, firms_ciks, columns, start_date, end_date)
    # pipeline.concatenate_dataframes(level="test", section="all", save_path=save_folder, start_date=start_date, end_date=end_date)
    # pipeline.aggregate_data(save_folder, firms_ciks) # Execute this after processing all CIKs
