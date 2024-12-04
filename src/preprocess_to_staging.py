import io
import pandas as pd
import boto3
import numpy as np
import tqdm
import joblib
from collections import OrderedDict
from sklearn.preprocessing import LabelEncoder
from numba import njit
import sqlite3

def preprocess_to_staging(
        bucket_raw, 
        # bucket_staging, 
        input_file, 
        # output_prefix
    ):
    """
    2. Créeez un script pour préparer les données au staging
    Ajoutez les étapes suivantes dans le fichier :
    • Télécharger les données depuis le bucket raw.
    • Nettoyer les données (suppression des doublons et des lignes vides).
    • Se connecter à la base MySQL distante.
    • Créer une table texts (si elle n'existe pas) avec les colonnes nécessaires.
    • Insérer les données nettoyées dans cette table.
    • Vérifier que les données sont bien insérées.
    """
    s3 = boto3.client('s3', endpoint_url='http://localhost:4566')

    # Step 1: Download raw data
    response = s3.get_object(Bucket=bucket_raw, Key=input_file)
    data = pd.read_parquet(io.BytesIO(response['Body'].read()))

    # Step 2: Handle missing values & duplicate values
    data = data.dropna()
    data = data.drop_duplicates()

    # Step 3: Connect to sqlite
    con = sqlite3.connect("staging.db")
    cur = con.cursor()
    
    # Step 4: Créer une table texts (si elle n'existe pas) avec les colonnes nécessaires.
    cur.execute("CREATE TABLE IF NOT EXISTS texts(text TEXT NOT NULL)")

    # Step 5: Insérer les données nettoyées dans cette table.
    # cur.executemany("INSERT INTO texts (text) VALUES (?)", [(text,) for text in data["text"]])
    # con.commit()

    # Step 6: Vérifier que les données sont bien insérées.
    cur.execute("SELECT COUNT(*) FROM texts WHERE text IS NOT NULL;")
    print(cur.fetchall())
    con.close()


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Preprocess data from raw to staging bucket")
    parser.add_argument("--bucket_raw", type=str, required=True, help="Name of the raw S3 bucket")
    # parser.add_argument("--bucket_staging", type=str, required=True, help="Name of the staging S3 bucket")
    parser.add_argument("--input_file", type=str, required=True, help="Name of the input file in raw bucket")
    # parser.add_argument("--output_prefix", type=str, required=True, help="Prefix for output files in staging bucket")
    args = parser.parse_args()

    preprocess_to_staging(args.bucket_raw, 
                        #   args.bucket_staging, 
                          args.input_file, 
                        #   args.output_prefix
                          )