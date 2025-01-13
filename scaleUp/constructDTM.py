from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit
from pyspark.sql.types import StructType, StructField, StringType
import sys
import os
# Add the parent directory to sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
# Import the function
import pandas as pd


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

    def csv_builder(self):
        """
        Build CSV files for each CIK using Spark.
        """
        def process_cik(cik, symbol):
            cik_path = os.path.join(self.data_folder, cik)
            if not os.path.exists(cik_path):
                print(f"Path does not exist for CIK: {cik}")
                return None

            # List all files for the current CIK
            files = [os.path.join(cik_path, f) for f in os.listdir(cik_path) if os.path.isfile(os.path.join(cik_path, f))]

            # Process each file and construct rows
            data = []
            for file_path in files:
                date = os.path.basename(file_path).split('.')[0]
                body = self.import_file(file_path)  # Replace with appropriate file reading logic
                data.append((symbol, cik, date, body))
                

            # Create a Spark DataFrame from the collected data
            if data:
                df = self.spark.createDataFrame(data, schema=self.columns)
                df = df.dropna(how="all", subset=df.columns)
                # Keep only relevant columns
                return df.select(["Name", "CIK", "Date", "Body"])
            
            return None

        for cik, symbol in self.firms_dict.items():
            output_path = os.path.join(self.output_folder, cik)
            if os.path.exists(output_path):
                print(f"CSV file already exists for CIK: {cik}, skipping...")
                continue

            print(f"Processing CIK: {cik}")
            cik_df = process_cik(cik, symbol)
            if cik_df:
                # Save the DataFrame as a CSV file, sorted by date
                cik_df = cik_df.orderBy(col("Date"))
                cik_df.write.csv(output_path, header=True, mode="overwrite")

        print(f"CSV files saved in: {self.output_folder}")

    def process_filings_for_cik_spark(self, spark, data_folder, files_path, firms_dict, firms_ciks, columns, start_date, end_date):
        """
        Process CIK filings using Spark in a distributed manner, applying the reader function to each file.
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
                file_path = os.path.join(cik_folder, file_name)
                if not file_name.endswith('.csv'):
                    continue
                # Apply the reader function
                processed_file = reader(file_name, file_loc=cik_folder)
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
            # combined_data = combined_data.merge(vol_data, how="inner", on="Date")
            combined_data = pd.merge(combined_data, vol_data, how="inner", on="Date")
            
            # Add the '_cik' column
            combined_data["_cik"] = cik

            # Reorder columns
            columns_to_move = ['_cik', '_vol', '_ret', '_vol+1', '_ret+1']
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

    def concatenate_dataframes(self):
        """
        Concatenate all processed DataFrames into a single DataFrame.
        """
        dataframes = [
            self.spark.read.csv(f"{self.save_folder}/processed/dtm_{cik}.csv", header=True, inferSchema=True)
            for cik in self.firms_ciks
        ]
        combined_df = dataframes[0].unionAll(*dataframes[1:])

        # Save the concatenated DataFrame
        output_path = os.path.join(self.save_folder, "final_combined.csv")
        combined_df.write.csv(output_path, header=True, mode="overwrite")
        print(f"Saved concatenated DataFrame to {output_path}")

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
    firms_csv_file_path = "/Users/apple/PROJECT/Code_4_SECfilings/sp500_total_constituents.csv"
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
    pipeline.process_filings_for_cik_spark(spark, data_folder, save_folder, firms_dict, firms_ciks, columns, start_date, end_date)
    pipeline.concatenate_dataframes()
