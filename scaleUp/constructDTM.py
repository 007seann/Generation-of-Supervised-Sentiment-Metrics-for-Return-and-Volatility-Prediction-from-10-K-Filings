
from pipeline import _cleanup_multiprocessing_resources, run_process_for_cik
from metadata import FileMetadata
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine

import pyspark.sql.functions as F

import sys
import os
import tqdm
import hashlib
import datetime
import logging
import atexit
import multiprocessing
import pandas as pd
import pyarrow.parquet as pq
import pyarrow as pa
import pyarrow.compute as pc
# Add the parent directory to sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
# Register the cleanup function to run at interpreter shutdown
atexit.register(_cleanup_multiprocessing_resources)

# Create a session maker using the pooled engine
# engine = get_engine()
# SessionLocal = sessionmaker(bind=engine)
# ------------------ SQLAlchemy Setup ------------------ #

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
        # self.redis_client = redis.Redis(host='localhost', port=6379, db=0)
        
        # --------------- Configure Database --------------- #
        # Adjust connection string for your environment
        db_url = "postgresql://apple:qwer@localhost:5432/seanchoimetadata"
        self.engine = create_engine(db_url, echo=False)
        self.SessionLocal = sessionmaker(bind=self.engine)
        Base = declarative_base()
        # self.SessionLocal = SessionLocal
        # Create table if not exists
        Base.metadata.create_all(self.engine)
        

        
    # ------------------- Helper Methods ------------------- #    
    @staticmethod
    def import_file(file_path):
        """
        Placeholder function for importing file content.
        Replace with the actual file reading logic.
        """
        with open(file_path, 'r', encoding='latin-1') as file:
            return file.read()
    @staticmethod
    def compute_file_hash(file_path, chunk_size=65536):
        """
        Compute an MD5 (or other) hash for file content to detect changes.
        """
        md5 = hashlib.md5()
        with open(file_path, 'rb') as f:
            while True:
                data = f.read(chunk_size)
                if not data:
                    break
                md5.update(data)
        return md5.hexdigest()
    @staticmethod
    def get_file_modified_time(file_path):
        """
        Return the last modification time of a file as a Python datetime.
        """
        epoch_time = os.path.getmtime(file_path)
        return datetime.datetime.fromtimestamp(epoch_time)

    def _scan_directory_and_update_db(self, directory_path, cik):
        """
        **Step 1: Pre-fetch Data from the Database**
        Scan directory and update metadata in PostgreSQL to identify new or changed files.
        """
        session = self.SessionLocal()
        newly_added_or_changed = []

        try:
            if not os.path.exists(directory_path):
                return []

            # Gather all files in the directory
            all_files = [
                os.path.join(directory_path, f)
                for f in os.listdir(directory_path)
                if os.path.isfile(os.path.join(directory_path, f)) and not f.endswith(".DS_Store")
            ]

            # Fetch metadata from PostgreSQL
            db_files = session.query(FileMetadata).filter(FileMetadata.is_deleted == False).all()
            db_file_map = {record.file_path: record for record in db_files}
            
            
            # Detect new and updated files
            for file_path in all_files:
                file_hash = self.compute_file_hash(file_path)
                last_modified = self.get_file_modified_time(file_path)
                existing_record = session.query(FileMetadata).filter(FileMetadata.file_path == file_path).first()
                if existing_record:
                    # Update existing record instead of inserting
                    existing_record.file_hash = file_hash
                    existing_record.last_modified = last_modified
                
                else:
                # if file_path not in db_file_map:
                    # New file
                    new_record = FileMetadata(
                        file_path=file_path,
                        last_modified=last_modified,
                        file_hash=file_hash,
                        is_deleted=False,
                        cik = cik
                    )
                    session.add(new_record)
                    newly_added_or_changed.append(file_path)
                    # Query immediately after committing
                    db_files = session.query(FileMetadata).filter(FileMetadata.is_deleted == False).all()
                    
                # elif (db_file_map[file_path].file_hash != file_hash or
                #     db_file_map[file_path].last_modified != last_modified):
                #     # Updated file
                #     record = db_file_map[file_path]
                #     record.file_hash = file_hash
                #     record.last_modified = last_modified
                #     newly_added_or_changed.append(file_path)

            # Mark deleted files
            db_files = session.query(FileMetadata).filter(FileMetadata.is_deleted == False, FileMetadata.cik == cik).all()
            db_file_map = {record.file_path: record for record in db_files}
            existing_files = set(all_files)
            for file_path, record in db_file_map.items():
                if file_path not in existing_files:
                    record.is_deleted = True

            session.flush()
            session.commit()

        except Exception as e:
            session.rollback()
            logging.error(f"Error scanning directory {directory_path}: {e}")
        finally:
            session.close()

        return newly_added_or_changed

    
    # ------------------- file_aggregator (Main Entry) ------------------- #
    def file_aggregator(self):
        """
        Build parquet files for each parquet using Spark, detecting changes via DB metadata.
        Only process & write out parquet for newly added or changed files.
        """
    
        for cik, symbol in self.firms_dict.items():
            cik_path = os.path.join(self.data_folder, cik)
            print('----------------------------------------------------------------')
            print(f"[file_aggregator] Processing parquet: {cik}")
            # 1) Scan the directory for new or changed files
            changed_files = self._scan_directory_and_update_db(cik_path, cik)
            print('changed files', changed_files)

            if not changed_files:
                print(f"[file_aggregator] No new or changed files for parquet: {cik}")
                continue

            # 2) Convert changed files to a Spark DataFrame
            #    (These are truly new or updated; we reprocess them.)
            new_data = []
            for file_path in changed_files:
                file_name = os.path.basename(file_path)
                date_str = file_name.split('.')[0]
                body = self.import_file(file_path)
                new_data.append((symbol, cik, date_str, body))

            # 3) Build Spark DF
            new_df = self.spark.createDataFrame(new_data, schema=self.columns)
            new_df = new_df.dropna(how="all", subset=new_df.columns)
            new_df = new_df.select(["Name", "CIK", "Date", "Body"])
            new_df = new_df.orderBy(col("Date"))  # Sort if needed

            # 4) Write the new files to parquet
            output_path = os.path.join(self.output_folder, cik)
            # We can append with coalesce(1) => single file per new batch, or multiple part files.
            new_df.coalesce(1).write.parquet(output_path, mode="append")

            print(f"[file_aggregator] Wrote/updated parquet for {len(changed_files)} file(s) under CIK: {cik}")

        print(f"[file_aggregator] parquet files saved/updated in: {self.output_folder}")


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
        
        if csv_files:
            merged = pd.concat(csv_files).reset_index(drop=True)
            merged.to_csv(os.path.join(files_path, "SP500.csv"), index=False)
            print(f"Aggregated CSV written to {os.path.join(files_path, 'SP500.csv')}")
        else:
            print("No CSV files found to aggregate.")
            
        
    # Function to add missing columns with double 0 values
    def add_missing_columns(self, df, columns):
        for column in columns:
            if column not in df.columns:
                df = df.withColumn(column, F.lit(0.0))
        return df

    # Function to align schemas of two DataFrames
    def align_schemas(self, df1, df2):
        df1_columns = set(df1.columns)
        df2_columns = set(df2.columns)
        
        df1 = self.add_missing_columns(df1, df2_columns - df1_columns)
        df2 = self.add_missing_columns(df2, df1_columns - df2_columns)
        
        return df1.select(sorted(df1.columns)), df2.select(sorted(df2.columns))
    
    def process_filings_for_cik_spark(self, save_folder, start_date, end_date):
        """
        Orchestrates the processing of CIK files:
        1. Distributes tasks to Spark workers using worker_process_cik.
        2. Collects metadata and Parquet file paths returned by workers.
        3. Optionally merges Parquet files for centralized output.
        """
        # Enable case sensitivity in Spark
        self.spark.conf.set("spark.sql.caseSensitive", "true")
        
        folder = 'company_df'
        folder_path = os.path.join(self.save_folder, folder)
        os.makedirs(folder_path, exist_ok=True)

        # 1) Distribute tasks to workers
        rdd = self.spark.sparkContext.parallelize(self.firms_ciks)
        db_url = "postgresql://apple:qwer@localhost:5432/seanchoimetadata"
        
        results = rdd.map(lambda cik: run_process_for_cik(
            cik,
            save_folder,
            folder_path,
            start_date,
            end_date,
            db_url
        )).collect()

         # Driver side: gather output file paths
        updated_files_paths = []
        for result in results:
            output_file = result["output_file"]
            if output_file:
                updated_files_paths.append(output_file)

        return updated_files_paths
    
    @staticmethod
    def combine_parquet_in_batches(file_paths, batch_size=50):
        # Process files in batches
        tables = []

        # Redirect standard output to suppress pyarrow.Table output
        original_stdout = sys.stdout
        sys.stdout = open(os.devnull, 'w')

        try:
            for i, file_path in enumerate(file_paths):
                table = pq.read_table(file_path)
                tables.append(table)

                # When the batch is full, write to disk
                if (i + 1) % batch_size == 0 or (i + 1) == len(file_paths):
                    combined_table = pa.concat_tables(tables, promote_options='default')
                    # Convert to Pandas DataFrame
                    df = combined_table.to_pandas()
                    columns_to_drop = ['form','table','content','heading']
                    df = df.drop(columns=columns_to_drop)
                    # Perform operations using Pandas
                    df = df.drop_duplicates(subset=["Date", "_cik", "_vol", "_ret"]).fillna(0.0)
                    
                    # Convert back to pyarrow.Table
                    combined_table = pa.Table.from_pandas(df)
                    tables = []  # Clear the batch
        finally:
            # Restore standard output
            sys.stdout.close()
            sys.stdout = original_stdout

        return combined_table

   
    def concatenate_dataframes(self, updated_paths, level, section, save_path, start_date, end_date):
        from hons_project.vol_reader_fun import vol_reader2
        from pyspark.sql.window import Window
        import pyspark.sql.functions as F
        """
        Concatenate all processed DataFrames into a single DataFrame using PySpark functions.
        """
        print('update_dataframes', updated_paths)
        # 1) Read in all Parquet files
        existing_files_path = os.path.join(save_path, 'processed')
        existing_files = os.listdir(existing_files_path)
        existing_files = [f for f in existing_files if f != '.DS_Store']
        existing_files = [os.path.join(existing_files_path, f) for f in existing_files]
        print('existing_file2222s', existing_files)
        
        # Combine Parquet files in batches
        combined_table = self.combine_parquet_in_batches(existing_files)
        
        # Convert the cleaned DataFrame back to a PySpark DataFrame
        combined_df = combined_table.to_pandas()

        if not os.path.exists(save_path):
            os.makedirs(save_path)
            

        # Generate 3-day rolling returns and volatilities for the provided firms' CIKs
        x1, x2 = vol_reader2(firms_ciks, start_date, end_date, window=3, extra_end=True, extra_start=True)

        # Shift the data by one time step to align with the desired time window
        x1 = x1.shift(1)
        x2 = x2.shift(1)

        # Slice the data to only include values within the start_date and end_date range
        x1 = x1[start_date:end_date]
        x2 = x2[start_date:end_date]

        # Initialize a flag to indicate the first iteration for appending data
        first = True

        # Loop through each firm CIK to align and merge 3-day rolling return/volatility with the main DataFrame
        for cik in firms_ciks:
            print(f'Aligning with 3-day return/volatility {cik}')
            
            # Extract the 3-day rolling return and volatility data for the current CIK
            x1c = x1[cik]
            x2c = x2[cik]
            
            # Concatenate the return and volatility data into a single DataFrame
            x = pd.concat([x1c, x2c], axis=1)
            x.columns = ['n_ret', 'n_vol']  # Rename columns for clarity
            
            # Filter rows in the combined DataFrame corresponding to the current CIK
            y = combined_df[combined_df['_cik'] == cik]
            
            # Set 'Date' as the index for both the main DataFrame and the return/volatility DataFrame
            y.set_index('Date', inplace=True)
            x.index = pd.to_datetime(x.index)
            y.index = pd.to_datetime(y.index)
            
            # Join the return/volatility data (`x`) with the filtered DataFrame (`y`) on the 'Date' index
            z = y.join(x)
            
            # Extract only the return and volatility columns from the joined data
            zz = z[['n_ret', 'n_vol']]
            
            # On the first iteration, initialize the combined DataFrame; otherwise, append to it
            if first:
                df_add = zz
                first = False
            else:
                df_add = pd.concat([df_add, zz], axis=0)

        # Reset the index of the combined DataFrame to include 'Date' as a regular column
        df_add.reset_index(inplace=True)

        # Ensure that the index of the combined DataFrame matches the original `combined_df`
        assert all(combined_df.index == df_add.index), 'Do not merge!'

        # Make a copy of the original `combined_df` to prevent modifications to the original
        combined_df = combined_df.copy()

        # Add new columns for the 3-day rolling return and volatility to the combined DataFrame
        combined_df['_ret'] = df_add['n_ret']
        combined_df['_vol'] = df_add['n_vol']


        filename = f'dtm_{level}_{section}'
        file_path = os.path.join(save_path, f"{filename}.parquet")
        dataframes = self.spark.createDataFrame(combined_df)
        dataframes.coalesce(1).write.parquet(file_path, mode='overwrite')

        print(f"Combined DataFrame saved to {file_path}")

# Example Usage
if __name__ == "__main__":
    # Initialize Spark session
    spark = (SparkSession.builder
        .appName("DataPipeline")
        .master("local[*]")
        # Memory allocations
        .config("spark.driver.memory", "8g")
        .config("spark.executor.memory", "8g")
        .getOrCreate()
    )

    # Define input parameters
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
    pipeline.file_aggregator()
    updated_paths = pipeline.process_filings_for_cik_spark(save_folder, start_date, end_date)
    pipeline.concatenate_dataframes(updated_paths=updated_paths,level="test", section="all", save_path=save_folder, start_date=start_date, end_date=end_date)


