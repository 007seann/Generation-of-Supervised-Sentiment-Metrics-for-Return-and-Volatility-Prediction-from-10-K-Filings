

import hashlib
import datetime
import pandas as pd
import multiprocessing
import logging

from metadata import FileMetadata
from sqlalchemy import create_engine

import sys
import os

# Add the parent directory of hons_project to the Python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# Late imports of your FileMetadata model if needed
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import SQLAlchemyError



def _cleanup_multiprocessing_resources():
    """
    Cleanup function to release any leftover multiprocessing resources
    like semaphores, locks, or Pools at program exit.
    """


    # Last resort: forcibly terminate active child processes
    for proc in multiprocessing.active_children():
        try:
            proc.terminate()
        except Exception as ex:
            pass

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
        # 2. Identify all parquet files
        all_files = [
            os.path.join(cik_folder, f)
            for f in os.listdir(cik_folder)
            if f.endswith('.parquet')
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


        processed_dataframes = [
            reader(os.path.basename(file_path), file_loc=cik_folder)
            for file_path in new_or_changed_files
            if reader(os.path.basename(file_path), file_loc=cik_folder) is not None
        ]

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
            
        # Combine and process the data
        combined = pd.concat(processed_dataframes)
        # Add volatility data
        vol_data = vol_reader(cik, start_date=start_date, end_date=end_date)
        combined.reset_index(inplace=True)
        combined = combined.copy()
        first_column_name = combined.columns[0]
        combined = pd.merge(combined.rename(columns={first_column_name: "Date"}), vol_data.reset_index(), how="inner", on="Date")
        
        # Add CIK column and reorder columns
        combined["_cik"] = cik
        columns_to_move = ['Date', '_cik', '_vol', '_ret', '_vol+1', '_ret+1']
        remaining_cols = [col for col in combined.columns if col not in columns_to_move]
        combined = combined[columns_to_move + remaining_cols]

        # Filter invalid rows and write to Parquet
        combined = combined[combined["_ret"].notna()]
        
        if processed_dataframes:
            # Update the existing dtm datasets
            existing_files = f"dtm_{cik}.parquet"
            existing_files_path = os.path.join(save_folder, 'processed', existing_files)
            if os.path.exists(existing_files_path):
                existing_files_df = pd.read_parquet(existing_files_path, engine='pyarrow')
                combined = pd.concat([existing_files_df, combined])
                combined.drop_duplicates(inplace=True)
                combined = combined.fillna(0.0)
            
        save_path = os.path.join(save_folder, 'processed')
        os.makedirs(save_path, exist_ok=True)
        out_file_path = os.path.join(save_path, f"dtm_{cik}.parquet")
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

# def worker_process_cik(cik, save_folder, folder_path, start_date, end_date, session_maker):
#     """
#     Processes files in folder_path/<CIK>:
#     1. Checks PostgreSQL for metadata to identify new or changed files.
#     2. Reads & processes only new/changed files.
#     3. Writes results to Parquet and updates metadata in PostgreSQL.
#     """
#     from hons_project.annual_report_reader import reader
#     from hons_project.vol_reader_fun import vol_reader
#     import pandas as pd
#     import os

#     cik_folder = os.path.join(folder_path, cik)
#     if not os.path.exists(cik_folder):
#         return {
#             "cik": cik,
#             "metadata": [],
#             "output_file": None  # No data returned
#         }

#     # Open a session with PostgreSQL
#     session = session_maker()
#     try:
#         # Fetch metadata for this CIK from PostgreSQL
#         existing_files = session.query(FileMetadata).filter(FileMetadata.file_path.like(f"{cik_folder}%")).all()
#         existing_metadata = {f.file_path: f for f in existing_files}

#         # Gather all CSV files in the folder
#         all_files = [
#             os.path.join(cik_folder, f)
#             for f in os.listdir(cik_folder)
#             if f.endswith('.parquet')
#         ]

#         # Lists for metadata and processed files
#         metadata_records = []
#         new_or_changed_files = []

#         # Identify new/changed files
#         for file_path in all_files:
#             file_hash = compute_file_hash(file_path)
#             last_modified = get_file_modified_time(file_path)

#             # Check if the file is new or changed
#             if (
#                 file_path not in existing_metadata or
#                 existing_metadata[file_path].file_hash != file_hash or
#                 existing_metadata[file_path].last_modified != last_modified
#             ):
#                 new_or_changed_files.append(file_path)

#             # Always update the metadata for this file
#             metadata_records.append({
#                 "file_path": file_path,
#                 "file_hash": file_hash,
#                 "last_modified": last_modified
#             })

#         # Process new/changed files
#         processed_dataframes = []
#         for file_path in new_or_changed_files:
#             file_name = os.path.basename(file_path)
#             df = reader(file_name, file_loc=cik_folder) # If executing the reader function with parquet files, then more efficient?
#             if df is not None:
#                 processed_dataframes.append(df)

#         # Combine processed files and add volatility data
#         if not processed_dataframes:
#             return {
#                 "cik": cik,
#                 "metadata": metadata_records,
#                 "output_file": None  # No data to write
#             }

#         combined = pd.concat(processed_dataframes)
#         vol_data = vol_reader(cik, start_date=start_date, end_date=end_date)
#         combined = pd.merge(combined, vol_data, how="inner", on="Date")
#         combined["_cik"] = cik
#         combined.reset_index(drop=True, inplace=True)

#         # Reorder columns
#         columns_to_move = ['Date', '_cik', '_vol', '_ret', '_vol+1', '_ret+1']
#         remaining_cols = [c for c in combined.columns if c not in columns_to_move]
#         combined = combined[columns_to_move + remaining_cols]

#         # Filter rows with missing _ret values
#         combined = combined[combined["_ret"].notnull()]

#         folder_name = 'processed'
#         save_path = os.path.join(save_folder, folder_name)
#         if not os.path.exists(save_path):
#             os.makedirs(save_path)
#         out_file_name = f"dtm_{cik}.parquet"
#         out_file_path = os.path.join(save_path, out_file_name)
#         combined.to_parquet(out_file_path, index=False)

#         # Write results to Parquet
#         # out_file_name = f"{cik}_processed.parquet"
#         # out_file_path = os.path.join(cik_folder, out_file_name)
#         # combined.to_parquet(out_file_path, index=False)
        
#         # Seems that process_cik and concatenate code should be combined?

#         # Update metadata in PostgreSQL
#         for m in metadata_records:
#             db_record = session.query(FileMetadata).filter_by(file_path=m["file_path"]).first()
#             if db_record:
#                 # Update existing record
#                 db_record.file_hash = m["file_hash"]
#                 db_record.last_modified = m["last_modified"]
#                 db_record.is_deleted = False
#             else:
#                 # Insert new record
#                 new_record = FileMetadata(
#                     file_path=m["file_path"],
#                     last_modified=m["last_modified"],
#                     file_hash=m["file_hash"],
#                     is_deleted=False,
#                     cik = cik
#                 )
#                 session.add(new_record)

#         session.commit()

#         return {
#             "cik": cik,
#             "metadata": metadata_records,
#             "output_file": out_file_path
#         }

#     except Exception as e:
#         session.rollback()
#         logging.error(f"Error processing CIK {cik}: {e}")
#         return {
#             "cik": cik,
#             "metadata": [],
#             "output_file": None
#         }
#     finally:
#         session.close()


# def worker_process_cik2(cik, folder_path, start_date, end_date):
#     """
#     The main worker function. It:
#     1) Scans all CSV files in the folder_path/<CIK> directory.
#     2) Applies "reader" and "vol_reader" logic (both must be serializable or importable).
#     3) Returns:
#         - A list of metadata records for centralized DB updates.
#         - Possibly the processed data as dictionaries to be merged or saved later.
#     """
#     from hons_project.annual_report_reader import reader
#     from hons_project.vol_reader_fun import vol_reader
#     import os

#     cik_folder = os.path.join(folder_path, cik)
#     if not os.path.exists(cik_folder):
#         # No data for this CIK
#         return {"cik": cik, "metadata": [], "processed_data": []}

#     # Gather all CSV files
#     all_files = [
#         os.path.join(cik_folder, f)
#         for f in os.listdir(cik_folder)
#         if f.endswith('.parquet')
#     ]

#     # Lists for metadata and processed data
#     metadata_records = []
#     processed_dataframes = []

#     for file_path in all_files:
#         # Collect file metadata
#         file_hash = compute_file_hash(file_path)
#         last_modified = get_file_modified_time(file_path)

#         metadata_records.append({
#             "file_path": file_path,
#             "file_hash": file_hash,
#             "last_modified": last_modified
#         })

#         # Apply "reader" logic
#         file_name = os.path.basename(file_path)
#         df = reader(file_name, file_loc=cik_folder)
#         if df is not None:
#             processed_dataframes.append(df)

#     # Merge processed data with volatility data
#     combined = None
#     if processed_dataframes:
#         combined = pd.concat(processed_dataframes)
#         vol_data = vol_reader(cik, start_date=start_date, end_date=end_date)
#         combined = pd.merge(combined, vol_data, how="inner", on="Date")
#         combined["_cik"] = cik
#         combined.reset_index(inplace=True)
        
#         # Reorder columns
#         columns_to_move = ['Date', '_cik', '_vol', '_ret', '_vol+1', '_ret+1']
#         new_column_order = columns_to_move + [col for col in combined.columns if col not in columns_to_move]
#         combined = combined[new_column_order]

#             # Filter rows with missing volatility data
#         combined = combined[combined["_ret"].notnull()]

#     return {
#         "cik": cik,
#         "metadata": metadata_records,        # For centralized DB updates
#         "processed_data": combined.to_dict("records") if combined is not None else []
#     }
