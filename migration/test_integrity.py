import os
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure
from bson.objectid import ObjectId
from bson.decimal128 import Decimal128
from datetime import datetime

def test_data_integrity(db_name, mongo_uri=None):
    """
    Effectue des tests d'intégrité des données sur MongoDB
    structurée en quatre collections : patients, admissions, insurances, treatments.

    :param db_name: Nom de la base de données MongoDB.
    :param mongo_uri: L'URI de connexion MongoDB.
    """
    if mongo_uri is None:
        mongo_uri = os.getenv('MONGO_URI', 'mongodb://localhost:27017/')

    print("Démarrage des tests d'intégrité des données pour le schéma multi-collections...")
    
    try:
        client = MongoClient(mongo_uri)
        db = client[db_name]
        print("Connexion à MongoDB réussie.")
    except ConnectionFailure as e:
        print(f"Échec de la connexion à MongoDB : {e}")
        return

    # Collections
    patients_col = db["patients"]
    admissions_col = db["admissions"]
    insurances_col = db["insurances"]
    treatments_col = db["treatments"]

    # Test 1: Comptage des documents
    print("\n Test 1: Comptage des documents")
    try:
        patient_count = patients_col.count_documents({})
        admission_count = admissions_col.count_documents({})
        insurance_count = insurances_col.count_documents({})
        treatment_count = treatments_col.count_documents({})

        print(f"Documents trouvés :")
        print(f"  - Patients: {patient_count}")
        print(f"  - Admissions: {admission_count}")
        print(f"  - Insurances: {insurance_count}")
        print(f"  - Treatments: {treatment_count}")

        assert patient_count > 0, "La collection 'patients' est vide."
        assert patient_count == admission_count == insurance_count == treatment_count, "Les collections n'ont pas le même nombre de documents."
        print("Test 1 Réussi : Toutes les collections ont le même nombre de documents et ne sont pas vides.")
    except Exception as e:
        print(f"Échec du Test 1 : {e}")
        client.close()
        return

    # Test 2: Intégrité Référentielle
    print("\n Test 2: Intégrité Référentielle")
    try:
        # On teste sur un échantillon pour ne pas surcharger
        sample_admission = admissions_col.find_one()
        if sample_admission:
            patient_id = sample_admission['patient_id']
            assert patients_col.count_documents({'_id': patient_id}) == 1, f"L'ID patient {patient_id} de 'admissions' n'existe pas dans 'patients'."
        
        sample_insurance = insurances_col.find_one()
        if sample_insurance:
            patient_id = sample_insurance['patient_id']
            assert patients_col.count_documents({'_id': patient_id}) == 1, f"L'ID patient {patient_id} de 'insurances' n'existe pas dans 'patients'."

        sample_treatment = treatments_col.find_one()
        if sample_treatment:
            patient_id = sample_treatment['patient_id']
            assert patients_col.count_documents({'_id': patient_id}) == 1, f"L'ID patient {patient_id} de 'treatments' n'existe pas dans 'patients'."
        
        print("Test 2 Réussi : L'intégrité référentielle est valide pour les documents échantillons.")
    except Exception as e:
        print(f"Échec du Test 2 : {e}")
        client.close()
        return

    # Test 3: Validation du Schéma
    print("\n Test 3: Validation du Schéma (Présence des champs)")
    try:
        patient_doc = patients_col.find_one()
        admission_doc = admissions_col.find_one()
        insurance_doc = insurances_col.find_one()
        treatment_doc = treatments_col.find_one()

        expected_patient_fields = ["name", "age", "gender", "blood_type", "medical_condition"]
        expected_admission_fields = ["patient_id", "date_of_admission", "discharge_date", "doctor", "hospital"]
        expected_insurance_fields = ["patient_id", "provider", "billing_amount"]
        expected_treatment_fields = ["patient_id", "medication", "test_results"]

        for field in expected_patient_fields:
            assert field in patient_doc, f"Champ manquant '{field}' dans 'patients'"
        print("Schéma 'patients' validé.")

        for field in expected_admission_fields:
            assert field in admission_doc, f"Champ manquant '{field}' dans 'admissions'"
        print("Schéma 'admissions' validé.")

        for field in expected_insurance_fields:
            assert field in insurance_doc, f"Champ manquant '{field}' dans 'insurances'"
        print("Schéma 'insurances' validé.")

        for field in expected_treatment_fields:
            assert field in treatment_doc, f"Champ manquant '{field}' dans 'treatments'"
        print("Schéma 'treatments' validé.")

        print("Test 3 Réussi : Les schémas des collections sont valides.")
    except Exception as e:
        print(f"Échec du Test 3 : {e}")
        client.close()
        return

    # Test 4: Validation des types de données
    print("\n Test 4: Validation des types de données")
    try:
        patient_doc = patients_col.find_one()
        admission_doc = admissions_col.find_one()
        insurance_doc = insurances_col.find_one()

        assert isinstance(patient_doc['age'], int),
        assert isinstance(admission_doc['date_of_admission'], datetime), 
        assert isinstance(insurance_doc['billing_amount'], Decimal128), 
        
        print("Test 4 Réussi : Les types de données sont valides pour les champs testés.")
    except Exception as e:
        print(f"Échec du Test 4 : {e}")
        client.close()
        return

    print("\n Tous les tests d'intégrité des données ont réussi !")
    client.close()

if __name__ == "__main__":
    DB_NAME = os.getenv('DB_NAME', 'healthcare')
    test_data_integrity(DB_NAME)