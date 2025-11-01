# Projet de migration de données médicales vers MongoDB via Docker

Ce projet contient un processus automatisé pour migrer des données médicales d'un fichier CSV vers une base de données MongoDB, le tout entièrement conteneurisé à l'aide de Docker et Docker Compose.

## Technologies

* Docker
* Docker Compose
* MongoDB
* Python (pymongo)

## Comment lancer la migration

L'ensemble du processus de migration est automatisé et peut être lancé avec une seule commande depuis la racine de ce projet.

```bash
docker-compose up -d --build
```

## Déroulement du processus de migration

Lors du lancement de la commande docker-compose up, la séquence d’événements suivante se produit :

1.  **Orchestration** : `docker-compose` lit le fichier `docker-compose.yml` et connecte les services au réseau.

2.  **Démarrage de la base de données** : Le service `mongodb` est démarré. Une base de données MongoDB 5.0 est initialisée et un volume nommé `mongo-data` est créé pour assurer la persistance des données.

3.  **Démarrage du service de migration** : Le service `migration_process` est démarré. Il exécute la commande suivante, qui enchaîne plusieurs scripts Python :

    *   **`clean_csv.py`** : Ce script lit le fichier source `healthcare_dataset.csv`, effectue des opérations de nettoyage et sauvegarde le résultat dans `healthcare_dataset_cleaned.csv`.

    *   **`migration.py`** : Le cœur du processus. Ce script se connecte à la base de données, vide la collection `patients` pour garantir une migration propre, lit le fichier `healthcare_dataset_cleaned.csv`, et insère les données dans la collection.

    *   **`test_integrity.py`** : Une fois la migration terminée, ce script exécute automatiquement une série de tests pour valider l'intégrité des données dans MongoDB.

4.  **Fin du processus** : Une fois tous les scripts exécutés avec succès, le conteneur `migration_process` s'arrête.


## Connexion avec MongoDB Compass

Pour visualiser les données dans MongoDB Compass :

- **Chaîne de connexion** :
  ```
  mongodb://root:password@localhost:27017/?authSource=admin
  ```
- **Configuration avancée** :
  - Allez dans l'onglet `More Options`.
  - Vérifiez que le paramètre **`TLS/SSL`** est bien sur **`None`**.

**Dépannage :**
Si la connexion échoue, assurez-vous qu'aucun autre service (comme une instance locale de MongoDB) n'utilise le port `27017` sur votre machine.


## Comment vérifier le résultat

*   **Consulter les logs** : Pour voir le détail de chaque étape du processus, utilisez la commande :
    ```bash
    docker-compose logs migration_process
    ```

*   **Inspecter la base de données (ligne de commande)** : Pour connecter directement à la base de données et voir un exemple de document inséré, utilisez la commande suivante :
    ```bash
    docker exec -it mongodb mongosh -u root -p password --eval "use healthcare; db.patients.findOne();"
    ```

## Comment nettoyer l'environnement

Pour arrêter et supprimer tous les conteneurs, réseaux et volumes créés par `docker-compose`, exécutez la commande :

```bash
docker-compose down -v
```

*   L'option `-v` est importante car elle supprime également le volume `mongo-data`, en assurant de repartir de zéro lors de la prochaine exécution.

## Schéma NoSQL (Collection: `patients`)

```mermaid
erDiagram
    PATIENTS {
        string _id
        string name
        int age
        string gender
        string blood_type
        string medical_condition
    }

    ADMISSION {
        string patient_id
        date date_of_admission
        string admission_type
        date discharge_date
        int room_number
        string doctor
        string hospital
    }

    INSURANCE {
        string patient_id
        string provider
        float billing_amount
    }

    TREATMENT {
        string patient_id
        string medication
        string test_results
    }

    PATIENTS ||--|| ADMISSION : has
    PATIENTS ||--|| INSURANCE : covered_by
    PATIENTS ||--|| TREATMENT : receives

```

## Gestion des rôles 

#### Le super-utilisateur `root`
L'utilisateur `root` est le super-administrateur dans MongoDB. 
- **Création** : Il est créé automatiquement au premier démarrage du service `mongodb` grâce aux variables d'environnement `MONGO_INITDB_ROOT_USERNAME` et `MONGO_INITDB_ROOT_PASSWORD` définies dans le `.env`.
- **Privilèges** : Il possède des droits illimités dans MongoDB. Son rôle est d'effectuer des tâches administratives de haut niveau, comme la création de bases de données, la gestion des utilisateurs et de leurs rôles. 

#### Autres rôles 
Le script `create_database_users.py` met en place des utilisateurs avec des rôles intégrés de MongoDB pour des besoins spécifiques :

- **`dbAdmin`** : Cet utilisateur peut gérer la structure de la base de données `healthcare` (créer/supprimer des collections, gérer les index), mais ne peut pas lire ou modifier les données des patients.
- **`readWrite`** : Cet utilisateur peut lire et modifier les données dans la base de données `healthcare`, mais ne peut pas altérer la structure de la base (comme supprimer une collection).

## Authentification

### Gestion des identifiants

Les informations d'identification (noms d'utilisateur, mots de passe) ne sont pas stockées en clair dans le code ou les fichiers de configuration. Elles sont gérées via des variables d'environnement, dans un fichier `.env`. Ce fichier est spécifiquement listé dans le `.gitignore` pour s'assurer qu'il ne soit jamais versionné, évitant ainsi toute fuite accidentelle d'informations sensibles.

### Chaîne de connexion (MONGO_URI)

Les scripts Python et les services utilisent ces variables d'environnement pour construire la chaîne de connexion `MONGO_URI`. Cette URI contient toutes les informations requises par le client pour s'authentifier auprès du serveur MongoDB.