import logging

# Custom logging filter to suppress specific log messages
class NoInsertFilter(logging.Filter):
    def filter(self, record):
        return 'INSERT INTO' not in record.getMessage()

# General logging configuration
logging.basicConfig(level=logging.WARNING)

# Suppress all SQLAlchemy verbose logs
logging.getLogger('sqlalchemy.engine').setLevel(logging.ERROR)
logging.getLogger('sqlalchemy.pool').setLevel(logging.ERROR)
logging.getLogger('sqlalchemy.dialects').setLevel(logging.ERROR)
logging.getLogger('sqlalchemy.orm').setLevel(logging.ERROR)

# Apply the custom filter to the SQLAlchemy engine logger
engine_logger = logging.getLogger('sqlalchemy.engine')
engine_logger.addFilter(NoInsertFilter())

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
import tqdm
import hashlib 
import datetime

# SQLAlchemy imports for DB-based metadatada
from sqlalchemy import create_engine, Column, String, DateTime, Boolean
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

# ------------------ SQLAlchemy Setup ------------------ #
Base = declarative_base()

class FileMetadata(Base):
    """
    Stores robust metadata about each file:
        - file_path: unique path or identifier (primary key)
        - last_modified: last modification time
        - file_hash: e.g., MD5 or other hash for detecting changes
        - is_deleted: True if no longer valid on disk
    """
    __tablename__ = 'file_metadata'

    file_path = Column(String, primary_key=True)
    last_modified = Column(DateTime, nullable=False)
    file_hash = Column(String, nullable=False)
    is_deleted = Column(Boolean, default=False)

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
        # Create table if not exists
        Base.metadata.create_all(self.engine)
        
        # Explicitly set logging level again before database operations
        logging.getLogger('sqlalchemy.engine.Engine').setLevel(logging.ERROR)
        logging.getLogger('sqlalchemy.engine.base.Engine').setLevel(logging.ERROR)
        logging.getLogger('sqlalchemy.engine.base').setLevel(logging.ERROR)
        
        
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

    def _scan_directory_and_update_db(self, directory_path):
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
                if os.path.isfile(os.path.join(directory_path, f))
            ]

            # Fetch metadata from PostgreSQL
            db_files = session.query(FileMetadata).filter(FileMetadata.is_deleted == False).all()
            db_file_map = {record.file_path: record for record in db_files}

            # Detect new and updated files
            for file_path in all_files:
                file_hash = self.compute_file_hash(file_path)
                last_modified = self.get_file_modified_time(file_path)

                if file_path not in db_file_map:
                    # New file
                    new_record = FileMetadata(
                        file_path=file_path,
                        last_modified=last_modified,
                        file_hash=file_hash,
                        is_deleted=False
                    )
                    session.add(new_record)
                    newly_added_or_changed.append(file_path)
                elif (db_file_map[file_path].file_hash != file_hash or
                    db_file_map[file_path].last_modified != last_modified):
                    # Updated file
                    record = db_file_map[file_path]
                    record.file_hash = file_hash
                    record.last_modified = last_modified
                    newly_added_or_changed.append(file_path)

            # Mark deleted files
            existing_files = set(all_files)
            for file_path, record in db_file_map.items():
                if file_path not in existing_files:
                    record.is_deleted = True

            session.commit()

        except Exception as e:
            session.rollback()
            logging.error(f"Error scanning directory {directory_path}: {e}")
        finally:
            session.close()

        return newly_added_or_changed
    # def _scan_directory_and_update_db(self, directory_path):
    #     """
    #     Scan a directory for new or changed files, updating the DB metadata.
    #     - If file doesn't exist in DB, insert a new record.
    #     - If file is changed (timestamp/hash), update the record.
    #     - If files are missing from disk, mark them is_deleted. (Optional)
    #     Returns a list of file paths that are newly added or changed (so we can reprocess them).
    #     """

    #     session = self.SessionLocal()
    #     newly_added_or_changed = []

    #     try:
    #         # 1) Gather all files from directory
    #         if not os.path.exists(directory_path):
    #             session.close()
    #             return []  # No directory => no files

    #         all_files = [
    #             os.path.join(directory_path, f)
    #             for f in os.listdir(directory_path)
    #             if os.path.isfile(os.path.join(directory_path, f))
    #         ]

    #         # 2) Convert them to a set for quick membership checks
    #         all_files_set = set(all_files)

    #         # 3) Pull existing DB records that haven't been marked deleted
    #         db_files = session.query(FileMetadata).filter(FileMetadata.is_deleted == False).all()
    #         db_paths_set = {record.file_path for record in db_files}

    #         # 4) Identify new vs. missing
    #         new_files = all_files_set - db_paths_set
    #         missing_files = db_paths_set - all_files_set

    #         # 5) Insert new files
    #         for fpath in new_files:
    #             file_hash = self.compute_file_hash(fpath)
    #             mtime = self.get_file_modified_time(fpath)
    #             new_record = FileMetadata(
    #                 file_path=fpath,
    #                 last_modified=mtime,
    #                 file_hash=file_hash,
    #                 is_deleted=False
    #             )
    #             session.add(new_record)
    #             newly_added_or_changed.append(fpath)

    #         # 6) Detect changed files among the intersection
    #         intersection_paths = all_files_set.intersection(db_paths_set)
    #         for record in db_files:
    #             if record.file_path in intersection_paths:
    #                 current_mtime = self.get_file_modified_time(record.file_path)
    #                 if record.last_modified != current_mtime:
    #                     # Possibly re-check content hash
    #                     current_hash = self.compute_file_hash(record.file_path)
    #                     if current_hash != record.file_hash:
    #                         record.file_hash = current_hash
    #                         newly_added_or_changed.append(record.file_path)
    #                     record.last_modified = current_mtime
    #                     session.add(record)

    #         # 7) Mark missing files as deleted (optional)
    #         # Some pipelines prefer to remove them entirely. YMMV.
    #         if missing_files:
    #             session.query(FileMetadata)\
    #                 .filter(FileMetadata.file_path.in_(missing_files))\
    #                 .update({FileMetadata.is_deleted: True}, synchronize_session=False)

    #         session.commit()

    #     except Exception as e:
    #         session.rollback()
    #         logging.error(f"Error scanning directory {directory_path}: {str(e)}")
    #     finally:
    #         session.close()

    #     return newly_added_or_changed

    
    # ------------------- csv_builder (Main Entry) ------------------- #
    def csv_builder(self):
        """
        Build CSV files for each CIK using Spark, detecting changes via DB metadata.
        Only process & write out CSV for newly added or changed files.
        """
    
        
        for cik, symbol in self.firms_dict.items():
            cik_path = os.path.join(self.data_folder, cik)
            print(f"[csv_builder] Processing CIK: {cik}")
            # 1) Scan the directory for new or changed files
            changed_files = self._scan_directory_and_update_db(cik_path)
            if not changed_files:
                print(f"[csv_builder] No new or changed files for CIK: {cik}")
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

            # 4) Write the new files to CSV
            output_path = os.path.join(self.output_folder, cik)
            # We can append with coalesce(1) => single file per new batch, or multiple part files.
            new_df.coalesce(1).write.csv(output_path, header=True, mode="overwrite")

            print(f"[csv_builder] Wrote/updated CSV for {len(changed_files)} file(s) under CIK: {cik}")

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
        
        if csv_files:
            merged = pd.concat(csv_files).reset_index(drop=True)
            merged.to_csv(os.path.join(files_path, "SP500.csv"), index=False)
            print(f"Aggregated CSV written to {os.path.join(files_path, 'SP500.csv')}")
        else:
            print("No CSV files found to aggregate.")

    def process_filings_for_cik_spark(self, spark, files_path, firms_ciks, start_date, end_date):
        """
        Incrementally update dtm_{cik}.csv using PostgreSQL metadata tracking.
        Only processes new, updated, or changed files.
        """
        folder = 'company_df'
        folder_path = os.path.join(files_path, folder)
        if not os.path.exists(folder_path):
            os.makedirs(folder_path, exist_ok=True)
            
        
        def process_single_cik(cik):
            """
            Process and update the `dtm_{cik}.csv` file for a single CIK.
            """
            import os
            import pandas as pd
            from hons_project.vol_reader_fun import vol_reader
            from hons_project.annual_report_reader import reader
            
            session = self.SessionLocal()
            # Define the output path for the processed CSV file
            output_path = os.path.join(files_path, f"processed/dtm_{cik}.csv")
            
            try:
                # Step 1: Load existing metadata for this CIK from PostgreSQL
                db_files = session.query(FileMetadata).filter(
                    FileMetadata.file_path.like(f"%/{cik}/%"),  # Match files under this CIK folder
                    FileMetadata.is_deleted == False
                ).all()

                # Create a set of file paths already in the DB
                db_files_map = {record.file_path: record for record in db_files}
                
                # Step 2: Gather all files in the CIK folder
                cik_folder = os.path.join(folder_path, cik)
                if not os.path.exists(cik_folder):
                    print(f"No folder found for CIK: {cik}")
                    return None

                all_files = [
                    os.path.join(cik_folder, file_name)
                    for file_name in os.listdir(cik_folder)
                    if file_name.endswith('.csv')
                ]
                
                # Step 3: Detect new and updated files
                new_files = []
                updated_files = []
                for file_path in all_files:
                    file_hash = self.compute_file_hash(file_path)
                    last_modified = self.get_file_modified_time(file_path)

                    if file_path not in db_files_map:
                        # New file
                        new_files.append(file_path)
                    elif (db_files_map[file_path].file_hash != file_hash or
                        db_files_map[file_path].last_modified != last_modified):
                        # Updated file
                        updated_files.append(file_path)
                        
                # Mark deleted files in the DB
                existing_file_paths = set(all_files)
                missing_files = set(db_files_map.keys()) - existing_file_paths
                if missing_files:
                    session.query(FileMetadata).filter(
                        FileMetadata.file_path.in_(missing_files)
                    ).update({FileMetadata.is_deleted: True}, synchronize_session=False)

                session.commit()
                
                # Step 4: Process new and updated files
                processed_data = []
                
                # for file_path in new_files + updated_files:
                #     file_name = os.path.basename(file_path)
                #     date_str = file_name.split('.')[0]
                #     body = self.import_file(file_path)
                #     processed_data.append((cik, date_str, body))
                
                # Add readers preprocessing procedures    
                for file_path in new_files + updated_files:
                    file_name = os.path.basename(file_path)
                    processed_file = reader(file_name, file_loc=cik_folder)
                    if processed_file is not None:
                        processed_data.append(processed_file)
                    

                    # Update or insert file metadata in DB
                    if file_path in db_files_map:
                        # Update existing record
                        record = db_files_map[file_path]
                        record.file_hash = file_hash
                        record.last_modified = last_modified
                    else:
                        # Insert new record
                        new_record = FileMetadata(
                            file_path=file_path,
                            last_modified=last_modified,
                            file_hash=file_hash,
                            is_deleted=False
                        )
                        session.add(new_record)

                session.commit()
                
                # Step 5: Preprocess and merge with volatility data
                if processed_data:
                    combined_data = pd.concat(processed_data)
                    vol_data = vol_reader(cik, start_date=start_date, end_date=end_date)

                    # Merge the data
                    combined_data = pd.merge(combined_data, vol_data, how="inner", on="Date")

                    # Add the '_cik' column
                    combined_data["_cik"] = cik

                    # Convert the index to a column and reorder columns
                    combined_data = combined_data.reset_index()
                    columns_to_move = ['Date', '_cik', '_vol', '_ret', '_vol+1', '_ret+1']
                    new_column_order = columns_to_move + [col for col in combined_data.columns if col not in columns_to_move]
                    combined_data = combined_data[new_column_order]

                    # Filter rows with missing volatility data
                    combined_data = combined_data[combined_data["_ret"].notnull()]

                    # Convert to Spark DataFrame
                    new_df = spark.createDataFrame(combined_data)

                    # Step 6: Merge with existing CSV data
                    if os.path.exists(output_path):
                        existing_df = spark.read.csv(output_path, header=True, inferSchema=True)
                        combined_df = existing_df.union(new_df).dropDuplicates(["Date", "_cik", "_vol", "_ret"])
                    else:
                        combined_df = new_df

                    # Write updated CSV to file
                    combined_df.coalesce(1).write.csv(output_path, header=True, mode="overwrite")
                    print(f"[process_filings_for_cik_spark] Updated dtm_{cik}.csv with {len(processed_data)} new/updated records.")
                else:
                    print(f"[process_filings_for_cik_spark] No new or updated files for CIK: {cik}")

            
            except Exception as e:
                session.rollback()
                print(f"Error processing CIK {cik}: {e}")
            finally:
                session.close()        

        
            # # Check if the output file already exists
            # # Not for real-time update 
            # # Need to be changed to update the dtm_{cik}.csv file
            # # Refer to def append_to_dtm code in the Notion
            # if os.path.exists(output_path):
            #     print(f"CSV file already exists for CIK: {cik}, skipping...")
            #     return None

            # cik_folder = os.path.join(folder_path, cik)
            # if not os.path.exists(cik_folder):
            #     print(f"No folder found for CIK: {cik}")
            #     return None

            # # Combine all processed files for the CIK into a single DataFrame
            # processed_data = []
            # for file_name in os.listdir(cik_folder):
            #     if not file_name.endswith('.csv'):
            #         continue
            #     # Apply the reader function
            #     processed_file = reader(file_name, file_loc=cik_folder)
            #     # processed_file.index = processed_file.index.tz_localize("UTC")

            #     if processed_file is not None:
            #         processed_data.append(processed_file)
            # if not processed_data:
            #     print(f"No valid files processed for CIK: {cik}")
            #     return None

            # # Combine all processed files into a DataFrames
            # combined_data = pd.concat(processed_data)
            # # Convert the processed data to a Spark DataFrame
            # # Read and join with volatility data

            # vol_data = vol_reader(cik, start_date=start_date, end_date=end_date)

            # # Merge the data
            # combined_data = pd.merge(combined_data, vol_data, how="inner", on="Date")

            # # Add the '_cik' column
            # combined_data["_cik"] = cik

            # # Convert the index to a column
            # combined_data = combined_data.reset_index()

            # # Reorder columns
            # columns_to_move = ['Date', '_cik', '_vol', '_ret', '_vol+1', '_ret+1']
            # new_column_order = columns_to_move + [col for col in combined_data.columns if col not in columns_to_move]
            # combined = combined_data[new_column_order]

            # # Filter rows with missing volatility data
            # combined_data = combined_data[combined_data["_ret"].notnull()]

            # # Save the processed DataFrame to a file
            # save_folder = os.path.join(files_path, "processed")
            # if not os.path.exists(save_folder):
            #     os.makedirs(save_folder)
            
            # combined.to_csv(output_path, index=False)
            
            # print(f"Processed CIK {cik} and saved to {output_path}")

            
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
    # pipeline.process_filings_for_cik_spark(spark, save_folder, firms_ciks, start_date, end_date)
    # pipeline.concatenate_dataframes(level="test", section="all", save_path=save_folder, start_date=start_date, end_date=end_date)
    # pipeline.aggregate_data(save_folder, firms_ciks) # Execute this after processing all CIKs
