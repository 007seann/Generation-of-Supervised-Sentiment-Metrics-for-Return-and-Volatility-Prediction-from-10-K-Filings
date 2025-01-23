import logging
import atexit
import multiprocessing

def _cleanup_multiprocessing_resources():
    """
    Cleanup function to release any leftover multiprocessing resources
    like semaphores, locks, or Pools at program exit.
    """
    # If you have a Pool, close and join it here, e.g.:
    # if your_pool_variable is not None:
    #     your_pool_variable.close()
    #     your_pool_variable.join()

    # Last resort: forcibly terminate active child processes
    for proc in multiprocessing.active_children():
        try:
            proc.terminate()
        except Exception as ex:
            pass

# Register the cleanup function to run at interpreter shutdown
atexit.register(_cleanup_multiprocessing_resources)


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
import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, StringType
import sys
import os
import tqdm


# Add the parent directory to sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))


# SQLAlchemy imports for DB-based metadatada
from sqlalchemy import create_engine, Column, String, DateTime, Boolean
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

# Configure the connection pool
def get_engine():
    db_url = "postgresql://apple:qwer@localhost:5432/seanchoimetadata"
    engine = create_engine(db_url, pool_size=10, max_overflow=5, echo=False)
    return engine

# Create a session maker using the pooled engine
engine = get_engine()
SessionLocal = sessionmaker(bind=engine)
# ------------------ SQLAlchemy Setup ------------------ #
Base = declarative_base()

# ------------------ Top Level Worker Functions ------------------ #
import os
import hashlib
import datetime
import pandas as pd

def compute_file_hash(file_path, chunk_size=65536):
    """Serializable helper function for computing file hash."""
    md5 = hashlib.md5()
    with open(file_path, 'rb') as f:
        while True:
            data = f.read(chunk_size)
            if not data:
                break
            md5.update(data)
    return md5.hexdigest()

def get_file_modified_time(file_path):
    """Serializable helper function for getting file modification time."""
    epoch_time = os.path.getmtime(file_path)
    return datetime.datetime.fromtimestamp(epoch_time)


def run_process_for_cik(cik, save_folder, folder_path, start_date, end_date, db_url):
    """
    Worker function invoked by Spark.
    1) Creates a SQLAlchemy session locally (no references to the driver session_maker).
    2) Checks PostgreSQL for file metadata for this CIK.
    3) Processes new/changed files, writes Parquet, updates metadata.
    """
    from hons_project.annual_report_reader import reader
    from hons_project.vol_reader_fun import vol_reader

    # Late imports of your FileMetadata model if needed
    from sqlalchemy import create_engine
    from sqlalchemy.orm import sessionmaker
    from sqlalchemy.exc import SQLAlchemyError
    engine = create_engine(db_url, echo=False)
    SessionLocal = sessionmaker(bind=engine)

    # -- Build paths --
    cik_folder = os.path.join(folder_path, cik)
    if not os.path.exists(cik_folder):
        return {
            "cik": cik,
            "metadata": [],
            "output_file": None  # No data returned
        }

    session = SessionLocal()
    try:
        # 1. Fetch existing file metadata from PostgreSQL
        existing_files = session.query(FileMetadata).filter(FileMetadata.file_path.like(f"{cik_folder}%")).all()
        existing_metadata = {f.file_path: f for f in existing_files}

        # 2. Identify all CSV files
        all_files = [
            os.path.join(cik_folder, f)
            for f in os.listdir(cik_folder)
            if f.endswith('.csv')
        ]

        metadata_records = []
        new_or_changed_files = []

        for file_path in all_files:
            file_hash = compute_file_hash(file_path)
            last_modified = get_file_modified_time(file_path)

            # If new or changed, mark for processing
            if (file_path not in existing_metadata
                or existing_metadata[file_path].file_hash != file_hash
                or existing_metadata[file_path].last_modified != last_modified):
                new_or_changed_files.append(file_path)

            metadata_records.append({
                "file_path": file_path,
                "file_hash": file_hash,
                "last_modified": last_modified
            })

        # 3. Process new/changed files
        processed_dataframes = []
        for file_path in new_or_changed_files:
            file_name = os.path.basename(file_path)
            df = reader(file_name, file_loc=cik_folder)
            if df is not None:
                processed_dataframes.append(df)

        if not processed_dataframes:
            # No new data to write
            for m in metadata_records:
                _upsert_metadata(session, m, cik)
            session.commit()
            return {
                "cik": cik,
                "metadata": metadata_records,
                "output_file": None
            }

        # Merge & add volatility data
        combined = pd.concat(processed_dataframes)
        vol_data = vol_reader(cik, start_date=start_date, end_date=end_date)
        combined = pd.merge(combined, vol_data, how="inner", on="Date")
        combined["_cik"] = cik
        combined.reset_index(drop=True, inplace=True)

        # Reorder columns
        columns_to_move = ['Date', '_cik', '_vol', '_ret', '_vol+1', '_ret+1']
        remaining_cols = [c for c in combined.columns if c not in columns_to_move]
        combined = combined[columns_to_move + remaining_cols]

        # Filter invalid rows
        combined = combined[combined["_ret"].notnull()]

        # Write to Parquet
        folder_name = 'processed'
        save_path = os.path.join(save_folder, folder_name)
        if not os.path.exists(save_path):
            os.makedirs(save_path)
        out_file_name = f"dtm_{cik}.parquet"
        out_file_path = os.path.join(save_path, out_file_name)
        combined.to_parquet(out_file_path, index=False)

        # Update metadata
        for m in metadata_records:
            _upsert_metadata(session, m, cik)

        session.commit()

        return {
            "cik": cik,
            "metadata": metadata_records,
            "output_file": out_file_path
        }

    except SQLAlchemyError as e:
        session.rollback()
        logging.error(f"[{cik}] DB Error: {e}")
        return {"cik": cik, "metadata": [], "output_file": None}
    except Exception as e:
        session.rollback()
        logging.error(f"[{cik}] Processing Error: {e}")
        return {"cik": cik, "metadata": [], "output_file": None}
    finally:
        session.close()


def _upsert_metadata(session, meta_dict, cik):
    """Helper to insert or update the FileMetadata row."""
    file_path = meta_dict["file_path"]
    record = session.query(FileMetadata).filter_by(file_path=file_path).first()
    if record:
        record.file_hash = meta_dict["file_hash"]
        record.last_modified = meta_dict["last_modified"]
        record.is_deleted = False
    else:
        new_record = FileMetadata(
            file_path=file_path,
            last_modified=meta_dict["last_modified"],
            file_hash=meta_dict["file_hash"],
            is_deleted=False,
            cik = cik
        )
        session.add(new_record)

def worker_process_cik(cik, save_folder, folder_path, start_date, end_date, session_maker):
    """
    Processes files in folder_path/<CIK>:
    1. Checks PostgreSQL for metadata to identify new or changed files.
    2. Reads & processes only new/changed files.
    3. Writes results to Parquet and updates metadata in PostgreSQL.
    """
    from hons_project.annual_report_reader import reader
    from hons_project.vol_reader_fun import vol_reader
    import pandas as pd
    import os

    cik_folder = os.path.join(folder_path, cik)
    if not os.path.exists(cik_folder):
        return {
            "cik": cik,
            "metadata": [],
            "output_file": None  # No data returned
        }

    # Open a session with PostgreSQL
    session = session_maker()
    try:
        # Fetch metadata for this CIK from PostgreSQL
        existing_files = session.query(FileMetadata).filter(FileMetadata.file_path.like(f"{cik_folder}%")).all()
        existing_metadata = {f.file_path: f for f in existing_files}

        # Gather all CSV files in the folder
        all_files = [
            os.path.join(cik_folder, f)
            for f in os.listdir(cik_folder)
            if f.endswith('.csv')
        ]

        # Lists for metadata and processed files
        metadata_records = []
        new_or_changed_files = []

        # Identify new/changed files
        for file_path in all_files:
            file_hash = compute_file_hash(file_path)
            last_modified = get_file_modified_time(file_path)

            # Check if the file is new or changed
            if (
                file_path not in existing_metadata or
                existing_metadata[file_path].file_hash != file_hash or
                existing_metadata[file_path].last_modified != last_modified
            ):
                new_or_changed_files.append(file_path)

            # Always update the metadata for this file
            metadata_records.append({
                "file_path": file_path,
                "file_hash": file_hash,
                "last_modified": last_modified
            })

        # Process new/changed files
        processed_dataframes = []
        for file_path in new_or_changed_files:
            file_name = os.path.basename(file_path)
            df = reader(file_name, file_loc=cik_folder) # If executing the reader function with parquet files, then more efficient?
            if df is not None:
                processed_dataframes.append(df)

        # Combine processed files and add volatility data
        if not processed_dataframes:
            return {
                "cik": cik,
                "metadata": metadata_records,
                "output_file": None  # No data to write
            }

        combined = pd.concat(processed_dataframes)
        vol_data = vol_reader(cik, start_date=start_date, end_date=end_date)
        combined = pd.merge(combined, vol_data, how="inner", on="Date")
        combined["_cik"] = cik
        combined.reset_index(drop=True, inplace=True)

        # Reorder columns
        columns_to_move = ['Date', '_cik', '_vol', '_ret', '_vol+1', '_ret+1']
        remaining_cols = [c for c in combined.columns if c not in columns_to_move]
        combined = combined[columns_to_move + remaining_cols]

        # Filter rows with missing _ret values
        combined = combined[combined["_ret"].notnull()]

        folder_name = 'processed'
        save_path = os.path.join(save_folder, folder_name)
        if not os.path.exists(save_path):
            os.makedirs(save_path)
        out_file_name = f"dtm_{cik}.parquet"
        out_file_path = os.path.join(save_path, out_file_name)
        combined.to_parquet(out_file_path, index=False)

        # Write results to Parquet
        # out_file_name = f"{cik}_processed.parquet"
        # out_file_path = os.path.join(cik_folder, out_file_name)
        # combined.to_parquet(out_file_path, index=False)
        
        # Seems that process_cik and concatenate code should be combined?

        # Update metadata in PostgreSQL
        for m in metadata_records:
            db_record = session.query(FileMetadata).filter_by(file_path=m["file_path"]).first()
            if db_record:
                # Update existing record
                db_record.file_hash = m["file_hash"]
                db_record.last_modified = m["last_modified"]
                db_record.is_deleted = False
            else:
                # Insert new record
                new_record = FileMetadata(
                    file_path=m["file_path"],
                    last_modified=m["last_modified"],
                    file_hash=m["file_hash"],
                    is_deleted=False,
                    cik = cik
                )
                session.add(new_record)

        session.commit()

        return {
            "cik": cik,
            "metadata": metadata_records,
            "output_file": out_file_path
        }

    except Exception as e:
        session.rollback()
        logging.error(f"Error processing CIK {cik}: {e}")
        return {
            "cik": cik,
            "metadata": [],
            "output_file": None
        }
    finally:
        session.close()


def worker_process_cik2(cik, folder_path, start_date, end_date):
    """
    The main worker function. It:
    1) Scans all CSV files in the folder_path/<CIK> directory.
    2) Applies "reader" and "vol_reader" logic (both must be serializable or importable).
    3) Returns:
        - A list of metadata records for centralized DB updates.
        - Possibly the processed data as dictionaries to be merged or saved later.
    """
    from hons_project.annual_report_reader import reader
    from hons_project.vol_reader_fun import vol_reader
    import os

    cik_folder = os.path.join(folder_path, cik)
    if not os.path.exists(cik_folder):
        # No data for this CIK
        return {"cik": cik, "metadata": [], "processed_data": []}

    # Gather all CSV files
    all_files = [
        os.path.join(cik_folder, f)
        for f in os.listdir(cik_folder)
        if f.endswith('.csv')
    ]

    # Lists for metadata and processed data
    metadata_records = []
    processed_dataframes = []

    for file_path in all_files:
        # Collect file metadata
        file_hash = compute_file_hash(file_path)
        last_modified = get_file_modified_time(file_path)

        metadata_records.append({
            "file_path": file_path,
            "file_hash": file_hash,
            "last_modified": last_modified
        })

        # Apply "reader" logic
        file_name = os.path.basename(file_path)
        df = reader(file_name, file_loc=cik_folder)
        if df is not None:
            processed_dataframes.append(df)

    # Merge processed data with volatility data
    combined = None
    if processed_dataframes:
        combined = pd.concat(processed_dataframes)
        vol_data = vol_reader(cik, start_date=start_date, end_date=end_date)
        combined = pd.merge(combined, vol_data, how="inner", on="Date")
        combined["_cik"] = cik
        combined.reset_index(inplace=True)
        
        # Reorder columns
        columns_to_move = ['Date', '_cik', '_vol', '_ret', '_vol+1', '_ret+1']
        new_column_order = columns_to_move + [col for col in combined.columns if col not in columns_to_move]
        combined = combined[new_column_order]

            # Filter rows with missing volatility data
        combined = combined[combined["_ret"].notnull()]

    return {
        "cik": cik,
        "metadata": metadata_records,        # For centralized DB updates
        "processed_data": combined.to_dict("records") if combined is not None else []
    }

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
    cik = Column(String, nullable=False)

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
        # db_url = "postgresql://apple:qwer@localhost:5432/seanchoimetadata"
        # self.engine = create_engine(db_url, echo=False)
        # self.SessionLocal = sessionmaker(bind=self.engine)
        self.SessionLocal = SessionLocal
        # Create table if not exists
        Base.metadata.create_all(engine)
        
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
            print("all_files", all_files)

            # Fetch metadata from PostgreSQL
            db_files = session.query(FileMetadata).filter(FileMetadata.is_deleted == False).all()
            db_file_map = {record.file_path: record for record in db_files}
            print('db_file_map', db_file_map)
            
            all_files_in_db = session.query(FileMetadata).all()
            print("All files in DB:", [record.file_path for record in all_files_in_db])
            print("All files_deleted in DB:", [record.is_deleted for record in all_files_in_db])
            
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
                    print('hihi@@@@@@@@@@@@', file_path)
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
                    print("Fetched files:", [f.file_path for f in db_files])
                    
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
            print('db_file_map', db_file_map)
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

    
    # ------------------- csv_builder (Main Entry) ------------------- #
    def csv_builder(self):
        """
        Build CSV files for each CIK using Spark, detecting changes via DB metadata.
        Only process & write out CSV for newly added or changed files.
        """
    
        for cik, symbol in self.firms_dict.items():
            cik_path = os.path.join(self.data_folder, cik)
            print('----------------------------------------------------------------')
            print(f"[csv_builder] Processing CIK: {cik}")
            # 1) Scan the directory for new or changed files
            changed_files = self._scan_directory_and_update_db(cik_path, cik)
            print('changed files', changed_files)

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
            new_df.coalesce(1).write.parquet(output_path, mode="append")

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
        def process_cik_wrapper(cik):
            return run_process_for_cik(cik, save_folder, folder_path, start_date, end_date, db_url)

        results = rdd.map(process_cik_wrapper).collect()

        # results = rdd.map(lambda cik: run_process_for_cik(
        #     cik,
        #     save_folder,
        #     folder_path,
        #     start_date,
        #     end_date,
        #     db_url
        # )).collect()

         # Driver side: gather output file paths
        all_parquet_files = []
        for result in results:
            output_file = result["output_file"]
            if output_file:
                all_parquet_files.append(output_file)

        return all_parquet_files

    
    def process_filings_for_cik_spark2(self, save_folder, start_date, end_date):
        """
        Distribute tasks to Spark workers, then centrally update DB and write CSVs.
        """
        # Enable case sensitivity in Spark
        self.spark.conf.set("spark.sql.caseSensitive", "true")
        
        folder = 'company_df'
        folder_path = os.path.join(save_folder, folder)
        os.makedirs(folder_path, exist_ok=True)

        # 1) Distribute tasks
        rdd = self.spark.sparkContext.parallelize(self.firms_ciks)
        # Map each CIK to the top-level worker function
        results = rdd.map(lambda cik: worker_process_cik2(
            cik,
            folder_path,
            start_date,
            end_date
        )).collect()

        # 2) Centralized Post-Processing: Update DB & Write CSVs
        session = self.SessionLocal()
        try:
            for result in results:
                cik = result["cik"]
                metadata = result["metadata"]
                records = result["processed_data"]  # list of dicts

                # (A) Update DB with new/updated metadata
                for m in metadata:
                    db_record = session.query(FileMetadata).filter_by(file_path=m["file_path"]).first()
                    if db_record:
                        # Update existing
                        db_record.file_hash = m["file_hash"]
                        db_record.last_modified = m["last_modified"]
                        db_record.is_deleted = False
                    else:
                        # Insert new
                        new_record = FileMetadata(
                            file_path=m["file_path"],
                            last_modified=m["last_modified"],
                            file_hash=m["file_hash"],
                            is_deleted=False,
                            cik = cik
                        )
                        session.add(new_record)

                # (B) Write/merge processed data into dtm_{cik}.csv
                folder_name = 'processed'
                folder_path = os.path.join(save_folder, folder_name)
                if not os.path.exists(folder_path):
                    os.makedirs(folder_path)
                output_path = os.path.join(save_folder, folder_name, f"dtm_{cik}.csv")

                if records:
                    # Normalize `Date` dynamically
                    normalized_records = [
                        {**record, "Date": record["Date"].strftime("%Y-%m-%d")} 
                        if hasattr(record["Date"], "strftime") else record
                        for record in records
                    ]
                    # Convert normalized records to Spark DataFrame
                    new_df = self.spark.createDataFrame(normalized_records)
                    # Ensure consistent schema before union
                    if os.path.exists(output_path):
                        existing_df = self.spark.read.csv(output_path, header=True, inferSchema=True)
                        
                        # Align schemas of existing_df and new_df
                        existing_df, new_df = self.align_schemas(existing_df, new_df)

                        # Merge DataFrames with consistent schema
                        combined_df = existing_df.union(new_df).dropDuplicates(["Date", "_cik", "_vol", "_ret"])
                    else:
                        combined_df = new_df

                    # Reorganize columns
                    columns_to_move = ['Date', '_cik', '_ret', '_ret+1', '_vol', '_vol+1']
                    remaining_columns = [col for col in combined_df.columns if col not in columns_to_move]
                    combined_df = combined_df.select(columns_to_move + remaining_columns)

                    # Write to CSV
                    combined_df.coalesce(1).write.csv(output_path, header=True, mode="overwrite")
                    logging.info(f"[process_filings_for_cik_spark] Updated dtm_{cik}.csv with {len(records)} records.")

            # Commit DB changes
            session.commit()
        except Exception as e:
            session.rollback()
            logging.error(f"Error in centralized DB update: {e}")
        finally:
            session.close()
    def concatenate_dataframes(self, file_paths, level, section, save_path, start_date, end_date):
        from hons_project.vol_reader_fun import vol_reader2
        from pyspark.sql.window import Window
        import pyspark.sql.functions as F
        """
        Concatenate all processed DataFrames into a single DataFrame using PySpark functions.
        """
        # Read all Parquet files into a single DataFrame
        dataframes = self.spark.read.parquet(*file_paths)

        if not dataframes:
            print("No dataframes to concatenate.")
            return

        # Remove duplicates and clean up columns
        dataframes = (
            dataframes
            .dropDuplicates(["Date", "_cik", "_vol", "_ret"])
            .fillna(0.0)
            .withColumnRenamed('Date0', 'Date')
            .orderBy("Date")
        )

        if not os.path.exists(save_path):
            os.makedirs(save_path)

        # Align with 3-day return/volatility
        # Generate n_ret (returns) and n_vol (volatility) using vol_reader2
        x1, x2 = vol_reader2(self.firms_ciks, start_date, end_date, window=3, extra_end=True, extra_start=True)

        # Convert x1 and x2 to PySpark DataFrames
        n_ret_df = self.spark.createDataFrame(x1.reset_index()).withColumnRenamed("index", "Date")
        n_vol_df = self.spark.createDataFrame(x2.reset_index()).withColumnRenamed("index", "Date")

        # Merge n_ret and n_vol into a single DataFrame
        vol_ret_df = (
            n_ret_df
            .join(n_vol_df, on="Date", how="inner")
            .withColumnRenamed("0", "n_ret")
            .withColumnRenamed("1", "n_vol")
        )

        # Process each CIK and join the return/volatility with the main DataFrame
        for cik in self.firms_ciks:
            print(f'Processing {cik}')
            cik_int = int(cik.lstrip('0'))

            # Filter for the current CIK
            y = dataframes.filter(dataframes['_cik'] == cik_int)

            # Join n_ret and n_vol with y based on the 'Date' column
            y = (
                y
                .join(vol_ret_df, on="Date", how="left")
                .orderBy("Date")
            )

            # If it's the first CIK, initialize combined_df
            if 'combined_df' not in locals():
                combined_df = y
            else:
                combined_df = combined_df.unionByName(y, allowMissingColumns=True)

        # Write the combined DataFrame to Parquet
        filename = f'dtm_{level}_{section}'
        file_path = os.path.join(save_path, f"{filename}.parquet")
        combined_df.write.parquet(file_path, mode='overwrite')
        print(f"Combined DataFrame saved to {file_path}")


    # def concatenate_dataframes2(self, level, section, save_path, start_date, end_date):
    #     from hons_project.vol_reader_fun import vol_reader2
    #     """
    #     Concatenate all processed DataFrames into a single DataFrame.
    #     """
    #     dataframes = []
    #     files_path = os.path.join(save_path, 'processed')
    #     for cik in self.firms_ciks:
    #         file_path = os.path.join(files_path, f'dtm_{cik}.csv')
    #         if os.path.exists(file_path):
    #             df = self.spark.read.csv(file_path, header=True, inferSchema=True)
    #             dataframes.append(df)
    #         else:
    #             print(f"File does not exist for CIK: {cik}, skipping...")

    #     if dataframes:
    #         # Combine all DataFrames
    #         combined_df = dataframes[0]
    #         for df in dataframes[1:]:
    #             combined_df = combined_df.unionByName(df, allowMissingColumns=True)

    #         combined_df = combined_df.fillna(0.0)
    #         combined_df = combined_df.withColumnRenamed('Date0', 'Date')
    #         combined_df = combined_df.orderBy("Date")

    #         if not os.path.exists(save_path):
    #             os.makedirs(save_path)

    #         # Align with 3-day return/volatility
    #         x1, x2 = vol_reader2(self.firms_ciks, start_date, end_date, window=3, extra_end=True, extra_start=True)
    #         x1 = x1.shift(1)
    #         x2 = x2.shift(1)
    #         x1 = x1[start_date:end_date]
    #         x2 = x2[start_date:end_date]


    #         first = True
    #         for cik in self.firms_ciks:
    #             print(f'Processing {cik}')
    #             x1c = x1[cik]
    #             x2c = x2[cik]
    #             x = pd.concat([x1c, x2c], axis=1)
    #             x.columns = ['n_ret', 'n_vol']
    #             y = combined_df.filter(combined_df['_cik'] == int(cik.lstrip('0')))

    #             y = y.withColumn('Date', y['Date'].cast('timestamp'))
    #             x.index = pd.to_datetime(x.index)
    #             y = y.toPandas()
    #             y.set_index('Date', inplace=True)
    #             z = y.join(x)
    #             zz = z[['n_ret', 'n_vol']]

    #             if first:
    #                 df_add = zz
    #                 first = False
    #             else:
    #                 df_add = pd.concat([df_add, zz], axis=0)

    #         df_add.reset_index(inplace=True)
    #         assert all(combined_df.toPandas().index == df_add.index), 'Do not merge!'

    #         combined_df = combined_df.toPandas()
    #         combined_df['_ret'] = df_add['n_ret']
    #         combined_df['_vol'] = df_add['n_vol']


    #         filename = f'dtm_{level}_{section}'
    #         file_path = os.path.join(save_path, f"{filename}.csv")
    #         combined_df.to_csv(file_path, index=False)
    #     else:
    #         print("No dataframes to concatenate.")




# Example Usage
if __name__ == "__main__":
    # Initialize Spark session
    spark = (SparkSession.builder
        .appName("DataPipeline")
        .master("local[*]")
        # Memory allocations
        .config("spark.driver.memory", "8g")
        .config("spark.executor.memory", "8g")
        # Optional: increase network timeout
        # .config("spark.network.timeout", "300s")
        # Optional: help with semaphore leaks
        # .config("spark.python.worker.reuse", "false")
        .getOrCreate()
    )

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
    # pipeline.process_filings_for_cik_spark2(save_folder, start_date, end_date)
    # pipeline.process_filings_for_cik_spark(spark, save_folder, firms_ciks, start_date, end_date)
    # pipeline.concatenate_dataframes(level="test", section="all", save_path=save_folder, start_date=start_date, end_date=end_date)
    # pipeline.aggregate_data(save_folder, firms_ciks) # Execute this after processing all CIKs



  
    # def process_filings_for_cik_spark(self, files_path, firms_ciks):
    #     """
    #     Incrementally update dtm_{cik}.csv using PostgreSQL metadata tracking.
    #     Handles new, updated, and deleted files. Ensures serialization safety.
    #     """
    #     folder = 'company_df'
    #     folder_path = os.path.join(files_path, folder)
    #     if not os.path.exists(folder_path):
    #         os.makedirs(folder_path, exist_ok=True)

    #     # Serialize-safe helper functions
    #     @staticmethod
    #     def compute_file_hash(file_path, chunk_size=65536):
    #         import hashlib
    #         md5 = hashlib.md5()
    #         with open(file_path, 'rb') as f:
    #             while True:
    #                 data = f.read(chunk_size)
    #                 if not data:
    #                     break
    #                 md5.update(data)
    #         return md5.hexdigest()

    #     @staticmethod
    #     def get_file_modified_time(file_path):
    #         import os
    #         import datetime
    #         epoch_time = os.path.getmtime(file_path)
    #         return datetime.datetime.fromtimestamp(epoch_time)

    #     def collect_metadata(cik, folder_path):
    #         """
    #         Worker-safe function to collect metadata for files in the CIK folder.
    #         Returns a list of metadata dictionaries and paths of processed files.
    #         """
    #         import os
    #         from hons_project.annual_report_reader import reader

    #         cik_folder = os.path.join(folder_path, cik)
    #         metadata = []
    #         processed_data = []

    #         if not os.path.exists(cik_folder):
    #             return metadata, processed_data

    #         try:
    #             # List all files in the folder
    #             all_files = [
    #                 os.path.join(cik_folder, file_name)
    #                 for file_name in os.listdir(cik_folder)
    #                 if file_name.endswith('.csv')
    #             ]

    #             # Process each file and collect metadata
    #             for file_path in all_files:
    #                 file_hash = compute_file_hash(file_path)
    #                 last_modified = get_file_modified_time(file_path)
    #                 file_name = os.path.basename(file_path)

    #                 # Read and process the file (safe logic here)
    #                 processed_file = reader(file_name, file_loc=cik_folder)
    #                 if processed_file is not None:
    #                     processed_data.append(processed_file)

    #                 # Collect metadata
    #                 metadata.append({
    #                     "file_path": file_path,
    #                     "file_hash": file_hash,
    #                     "last_modified": last_modified,
    #                 })

    #         except Exception as e:
    #             print(f"Error processing CIK {cik}: {e}")

    #         return metadata, processed_data

    #     def process_single_cik(cik):
    #         """
    #         Worker-safe function to process a single CIK and return metadata + processed data.
    #         """
    #         import pandas as pd
    #         from hons_project.vol_reader_fun import vol_reader

    #         cik_metadata, cik_processed_data = collect_metadata(cik, folder_path)

    #         if cik_processed_data:
    #             combined_data = pd.concat(cik_processed_data)
    #             vol_data = vol_reader(cik, start_date=self.start_date, end_date=self.end_date)
    #             combined_data = pd.merge(combined_data, vol_data, how="inner", on="Date")
    #             combined_data["_cik"] = cik
    #             combined_data = combined_data.reset_index()

    #             # Output combined data as a dictionary for centralized updates
    #             return {
    #                 "metadata": cik_metadata,
    #                 "processed_data": combined_data,
    #                 "cik": cik
    #             }
    #         return {
    #             "metadata": cik_metadata,
    #             "processed_data": None,
    #             "cik": cik
    #         }

    #     # Parallel processing of CIKs
    #     cik_rdd = self.spark.sparkContext.parallelize(firms_ciks)
    #     cik_results = cik_rdd.map(process_single_cik).collect()

    #     # Centralized post-processing
    #     session = self.SessionLocal()
    #     try:
    #         for result in cik_results:
    #             metadata = result["metadata"]
    #             processed_data = result["processed_data"]
    #             cik = result["cik"]
    #             output_path = os.path.join(files_path, f"processed/dtm_{cik}.csv")

    #             # Update metadata in the database
    #             for record in metadata:
    #                 db_record = session.query(FileMetadata).filter_by(file_path=record["file_path"]).first()
    #                 if db_record:
    #                     db_record.file_hash = record["file_hash"]
    #                     db_record.last_modified = record["last_modified"]
    #                 else:
    #                     new_record = FileMetadata(
    #                         file_path=record["file_path"],
    #                         last_modified=record["last_modified"],
    #                         file_hash=record["file_hash"],
    #                         is_deleted=False
    #                     )
    #                     session.add(new_record)
    #             session.commit()

    #             # Save processed data
    #             if processed_data is not None:
    #                 # Write the combined data to a CSV file
    #                 processed_df = self.spark.createDataFrame(processed_data)
    #                 if os.path.exists(output_path):
    #                     existing_df = self.spark.read.csv(output_path, header=True, inferSchema=True)
    #                     combined_df = existing_df.union(processed_df).dropDuplicates(["Date", "_cik", "_vol", "_ret"])
    #                 else:
    #                     combined_df = processed_df
    #                 combined_df.coalesce(1).write.csv(output_path, header=True, mode="overwrite")
    #                 print(f"[process_filings_for_cik_spark] Updated dtm_{cik}.csv with new/updated records.")
    #             else:
    #                 print(f"[process_filings_for_cik_spark] No new or updated records for CIK {cik}.")
    #     except Exception as e:
    #         session.rollback()
    #         print(f"Error updating database metadata: {e}")
    #     finally:
    #         session.close()

        
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

            
        # # Parallel processing of all CIKs
        # cik_rdd = spark.sparkContext.parallelize(firms_ciks)
        # cik_rdd.foreach(lambda cik: process_single_cik(cik))