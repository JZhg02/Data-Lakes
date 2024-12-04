# import sqlite3
# # Connexion `a une base de donn´ees (ou cr´eation si elle n'existe pas)
# conn = sqlite3.connect('staging.db')
# cursor = conn.cursor()
# # Cr´eation d'une table de test
# conn.commit()
# # Interrogation de la base
# cursor.execute('SELECT * FROM test_table')
# print(cursor.fetchall())
# conn.close()


import pymongo

# Connect to the MongoDB server
client = pymongo.MongoClient("mongodb://localhost:27017/")  # Replace with your MongoDB URI if needed

# Access the 'curated' database
curated_db = client["curated"]

# Access the 'wikitext' collection
wikitext_collection = curated_db["wikitext"]

# Retrieve all documents in the collection
documents = wikitext_collection.find().limit(5)

for doc in documents:
    print(doc)

# Count documents where the "source" field is "Wikipedia"
count = wikitext_collection.count_documents({})
print(f"Documents count: {count}")

# wikitext_collection.drop()

# Close the connection
client.close()