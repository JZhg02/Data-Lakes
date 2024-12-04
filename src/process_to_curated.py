import pandas as pd
from transformers import AutoTokenizer
import sqlite3
from pymongo import MongoClient
import datetime

def tokenize_sequences(
        # bucket_curated, 
        uri, 
        # output_file, 
        model_name="openai-community/gpt2"
    ):
    """
    • Connexion à MySQL : Récupérez les données depuis la table texts créée à l'étape précédente.
    • Tokenisation des textes : Utilisez le tokenizer de votre choix pour tokenizer les données. 
    Si vous avez du mal à choisir, gpt2 ou distilbert-base-uncased peuvent être de bonnes options.
    • Connexion à MongoDB : Connectez-vous à la base MongoDB locale.
    • Insertion dans la collection MongoDB : Créez une collection appelée wikitext dans la base curated et insérez les données tokenisées
    tout en conservant les métadonnées.
    """
    
    # Step 1: Connexion à MySQL : Récupérez les données depuis la table texts créée à l'étape précédente.
    con = sqlite3.connect("staging.db")
    cur = con.cursor()
    cur.execute("SELECT * FROM texts")
    rows = cur.fetchall()

    print(f"Loading tokenizer for {model_name}...")
    tokenizer = AutoTokenizer.from_pretrained(model_name)
    tokenizer.pad_token = tokenizer.eos_token

    # Step 2: Connexion à MongoDB : Connectez-vous à la base MongoDB locale.
    try:
        client = MongoClient(uri.replace("http", "mongodb"))
        client.admin.command("ping")
        print("Connected successfully")
    except Exception as e:
        raise Exception("The following error occurred: ", e)
    
    database = client["curated"]

    if "wikitext" in database.list_collection_names():
        wikitext_collection = database.get_collection("wikitext")
    else:
        wikitext_collection = database.create_collection("wikitext")


    for i in range(len(rows)):
        # Step 3: Tokenisation des textes : Utilisez le tokenizer de votre choix pour tokenizer les données. 
        tokens = tokenizer(rows[i], truncation=True, padding=True, max_length=128)["input_ids"]
        print(tokens)
        wikitext_collection.insert_one(
            {
                "id": f"id-{i}",
                "text": rows[i],
                "tokens": tokens,
                "metadata": {
                    "source": "sqlite3",
                    "processed_at": datetime.datetime.now()
                }
            }
        )
        document = wikitext_collection.find_one({"id":f"id-{i}"})
        print(f"Added document:{document}")
    cur.close()
    con.close()

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Process data from staging to curated bucket")
    # parser.add_argument("--bucket_curated", type=str, required=True, help="Name of the curated S3 bucket")
    # parser.add_argument("--input_file", type=str, required=True, help="Name of the input file in the staging bucket")
    # parser.add_argument("--output_file", type=str, required=True, help="Name of the output file in the curated bucket")
    parser.add_argument("--uri", type=str, help="mongodb_uri")
    parser.add_argument("--model_name", type=str, default="openai-community/gpt2", help="Tokenizer model name")
    args = parser.parse_args()

    tokenize_sequences(
        # args.bucket_staging, 
        # args.bucket_curated, 
        # args.input_file, 
        # args.output_file, 
        args.uri,
        args.model_name
    )
