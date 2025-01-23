from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf, explode
from pyspark.sql.types import ArrayType, StringType, FloatType
from pyspark import SparkFiles
import nltk
import os
import logging
from collections import Counter
import re

# Initialize Spark session
spark = SparkSession.builder.appName("DocumentTermMatrix").getOrCreate()

# Distribute NLTK resources
nltk_data_path = "/Users/apple/PROJECT/package"  # Path where NLTK data is downloaded
if not os.path.exists(nltk_data_path):
    nltk.download('stopwords', download_dir=nltk_data_path)
    nltk.download('words', download_dir=nltk_data_path)
    nltk.download('punkt', download_dir=nltk_data_path)
    nltk.download('wordnet', download_dir=nltk_data_path)

spark.sparkContext.addFile(nltk_data_path, recursive=True)

# Broadcast resources to workers (stopwords, words)
stopwords_set = set(nltk.corpus.stopwords.words('english'))
words_set = set(nltk.corpus.words.words())

broadcast_stopwords = spark.sparkContext.broadcast(stopwords_set)
broadcast_words_set = spark.sparkContext.broadcast(words_set)

# UDF to clean and tokenize text
def clean_and_tokenize(text):
    try:
        nltk.data.path.append(SparkFiles.get("nltk_data"))  # Set NLTK data path on workers

        # Initialize worker-local resources
        lemmatizer = nltk.stem.WordNetLemmatizer()

        terms = nltk.word_tokenize(text.lower())
        # Remove non-alphabetic tokens
        terms = [w for w in terms if re.match(r'[^\W\d]*$', w)]
        # Remove stopwords
        terms = [w for w in terms if w not in broadcast_stopwords.value]
        # Lemmatize
        terms = [lemmatizer.lemmatize(w) for w in terms]
        # Keep only valid English words
        terms = [w for w in terms if w in broadcast_words_set.value]
        return terms
    except Exception as e:
        logging.error(f"Error during text cleaning: {e}")
        return []

clean_and_tokenize_udf = udf(clean_and_tokenize, ArrayType(StringType()))

def reader(file_name, file_loc):
    file_path = f'{file_loc}/{file_name}'

    try:
        # Read the Parquet file into a PySpark DataFrame
        df = spark.read.parquet(file_path)
    except Exception as e:
        logging.error(f"Error reading file {file_path}: {e}")
        return None, None

    # Assume the column "Body" contains the text and "Date" contains the document date
    print(f'--- Total Articles: {df.count()} ---')

    # Clean and tokenize text
    df = df.withColumn("tokens", clean_and_tokenize_udf(col("Body")))

    # Explode tokens to create a vocabulary
    vocab_df = df.select(explode(col("tokens")).alias("token"))
    vocab_df = vocab_df.groupBy("token").count()

    # Remove tokens that are not alphabetic or are stopwords
    vocab_df = vocab_df.filter(~col("token").rlike(r'[\\W\\d]'))
    vocab_df = vocab_df.filter(~col("token").isin(broadcast_stopwords.value))
    
    # Collect vocabulary
    vocab = [row["token"] for row in vocab_df.collect()]
    print(f'- Vocab size: {len(vocab)}')

    # Construct Document-Term Matrix (DTM)
    print('Constructing document-term matrix')

    # Function to count terms in each document
    @udf(ArrayType(FloatType()))
    def calculate_term_vector(tokens):
        term_counts = Counter(tokens)
        return [float(term_counts.get(term, 0)) for term in vocab]

    # Apply the term vector calculation
    df = df.withColumn("term_vector", calculate_term_vector(col("tokens")))

    # Create a DataFrame with the term matrix
    D_df = df.select(col("Date"), col("term_vector"))

    print('Document-term matrix constructed')

    return D_df, vocab
