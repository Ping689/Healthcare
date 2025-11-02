import csv
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure
from bson.objectid import ObjectId
from bson.decimal128 import Decimal128
from decimal import Decimal
from datetime import datetime
import os

def migrate_csv_to_mongodb(csv_file_path, db_name, mongo_uri):
    """
    Migre les données d'un fichier CSV vers MongoDB
    avec plusieurs collections (Patients, Admissions, Insurances, Treatments).

    :param csv_file_path: Chemin vers le fichier CSV nettoyé.
    :param db_name: Nom de la base de données MongoDB.
    :param mongo_uri: L'URI de connexion MongoDB.
    """
    print("Démarrage du processus de migration multi-collections...")

    try:
        client = MongoClient(mongo_uri)
        client.admin.command('ping')
        print("Connecté avec succès à MongoDB.")
    except ConnectionFailure as e:
        print(f"Impossible de se connecter à MongoDB : {e}")
        return

    db = client[db_name]
    
    # Définition des collections
    patients_collection = db["patients"]
    admissions_collection = db["admissions"]
    insurances_collection = db["insurances"]
    treatments_collection = db["treatments"]

    collections = {
        "patients": patients_collection,
        "admissions": admissions_collection,
        "insurances": insurances_collection,
        "treatments": treatments_collection
    }

    # Nettoyage des collections existantes
    for name, collection in collections.items():
        print(f"Nettoyage de la collection : {name}...")
        collection.delete_many({})

    print(f"Lecture des données depuis {csv_file_path} et insertion dans les collections...")
    
    try:
        with open(csv_file_path, mode='r', encoding='utf-8') as csv_file:
            csv_reader = csv.DictReader(csv_file)
            
            total_rows = 0
            for row in csv_reader:
                try:
                    # Créer et insérer le document patient
                    patient_doc = {
                        "name": row["Name"],
                        "age": int(row["Age"]),
                        "gender": row["Gender"],
                        "blood_type": row["Blood Type"],
                        "medical_condition": row["Medical Condition"]
                    }
                    patient_id = patients_collection.insert_one(patient_doc).inserted_id

                    # Créer et insérer le document admission
                    admission_doc = {
                        "patient_id": patient_id,
                        "date_of_admission": datetime.strptime(row["Date of Admission"], '%Y-%m-%d'),
                        "admission_type": row["Admission Type"],
                        "discharge_date": datetime.strptime(row["Discharge Date"], '%Y-%m-%d'),
                        "room_number": int(row.get("Room Number", 0)),
                        "doctor": row["Doctor"],
                        "hospital": row["Hospital"]
                    }
                    admissions_collection.insert_one(admission_doc)

                    # Créer et insérer le document assurance
                    insurance_doc = {
                        "patient_id": patient_id,
                        "provider": row["Insurance Provider"],
                        "billing_amount": Decimal128(Decimal(row["Billing Amount"]))
                    }
                    insurances_collection.insert_one(insurance_doc)

                    # Créer et insérer le document traitement
                    treatment_doc = {
                        "patient_id": patient_id,
                        "medication": row["Medication"],
                        "test_results": row["Test Results"]
                    }
                    treatments_collection.insert_one(treatment_doc)
                    
                    total_rows += 1

                except (ValueError, TypeError, KeyError) as e:
                    print(f"Ligne ignorée en raison d'une erreur de conversion ou de clé manquante : {row} - Erreur : {e}")
                    continue
            
            print(f"{total_rows} patients et leurs données associées ont été insérés avec succès.")

    except FileNotFoundError:
        print(f"Erreur : Le fichier {csv_file_path} n'a pas été trouvé.")
    except Exception as e:
        print(f"Une erreur s'est produite lors de la migration : {e}")
    finally: 
        client.close()
        print("Connexion MongoDB fermée.")

if __name__ == "__main__":
    CSV_FILE = os.getenv('OUTPUT_CSV_FILE', 'healthcare_dataset_cleaned.csv')
    DB_NAME = os.getenv('DB_NAME', 'healthcare')
    MONGO_URI = os.getenv('MONGO_URI', 'mongodb://localhost:27017/')
    
    migrate_csv_to_mongodb(CSV_FILE, DB_NAME, MONGO_URI)