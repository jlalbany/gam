# Scripts de déploiement

Ce dossier contient tous les scripts nécessaires pour déployer l'infrastructure GAM sur Google Cloud Platform.

## Scripts disponibles

### setup_all.sh (Recommandé)
Script tout-en-un qui exécute l'ensemble du processus de déploiement.

**Usage** :
```bash
export GCP_PROJECT_ID="your-project-id"
export REGION="europe-west1"  # Optionnel
export SERVICE_ACCOUNT="gam-reporter@your-project-id.iam.gserviceaccount.com"
./setup_all.sh
```

### setup_bigquery.sh
Crée le dataset BigQuery et les tables partitionnées.

**Ce qu'il fait** :
- Crée le dataset `gam_data` (location: EU par défaut)
- Crée la table `report_inventory_daily` (partitionnée par date)
- Crée la table `report_geo_monthly` (partitionnée par report_date)

**Usage** :
```bash
export GCP_PROJECT_ID="your-project-id"
./setup_bigquery.sh
```

### deploy.sh
Déploie la Cloud Function Gen 2.

**Configuration** :
- Runtime: Python 3.11
- Mémoire: 1024MB
- Timeout: 540 secondes (9 minutes)
- Trigger: HTTP (authentifié)

**Usage** :
```bash
export GCP_PROJECT_ID="your-project-id"
export REGION="europe-west1"
export SERVICE_ACCOUNT="gam-reporter@your-project-id.iam.gserviceaccount.com"
./deploy.sh
```

### create_schedulers.sh
Configure les Cloud Scheduler jobs.

**Jobs créés** :
1. `trigger-gam-inventory-daily` - Quotidien à 6h00
2. `trigger-gam-geo-monthly` - Mensuel le 1er à 7h00

**Usage** :
```bash
export GCP_PROJECT_ID="your-project-id"
export REGION="europe-west1"
export SERVICE_ACCOUNT="gam-reporter@your-project-id.iam.gserviceaccount.com"
./create_schedulers.sh
```

## Ordre d'exécution manuel

Si vous préférez exécuter les scripts un par un :

1. `./setup_bigquery.sh` - Créer les tables
2. `./deploy.sh` - Déployer la fonction
3. `./create_schedulers.sh` - Configurer les schedulers

## Variables d'environnement requises

| Variable | Description | Défaut | Requis |
|----------|-------------|--------|--------|
| GCP_PROJECT_ID | Project ID GCP | - | ✅ |
| REGION | Région GCP | europe-west1 | ❌ |
| SERVICE_ACCOUNT | Service Account email | gam-reporter@PROJECT.iam... | ❌ |

## Prérequis

Avant d'exécuter ces scripts :

1. **gcloud CLI installé et configuré** :
   ```bash
   gcloud auth login
   gcloud config set project YOUR_PROJECT_ID
   ```

2. **Permissions nécessaires** :
   - Cloud Functions Admin
   - Cloud Scheduler Admin
   - BigQuery Admin
   - Service Account User

3. **Secret Manager configuré** :
   Le secret `gam_api_config` doit exister avec les credentials GAM

4. **APIs activées** :
   ```bash
   gcloud services enable cloudfunctions.googleapis.com
   gcloud services enable cloudscheduler.googleapis.com
   gcloud services enable bigquery.googleapis.com
   gcloud services enable secretmanager.googleapis.com
   ```

## Dépannage

### Erreur : "API not enabled"
```bash
gcloud services enable cloudfunctions.googleapis.com cloudscheduler.googleapis.com
```

### Erreur : "Permission denied"
Vérifier que vous avez les rôles nécessaires :
```bash
gcloud projects get-iam-policy YOUR_PROJECT_ID \
  --flatten="bindings[].members" \
  --filter="bindings.members:user:YOUR_EMAIL"
```

### Erreur : "Function deployment timeout"
Augmenter le timeout dans deploy.sh ou réessayer.

## Redéploiement

Pour mettre à jour une fonction existante, relancez simplement :
```bash
./deploy.sh
```

Pour recréer les schedulers :
```bash
# Les supprimer d'abord
gcloud scheduler jobs delete trigger-gam-inventory-daily --location=europe-west1
gcloud scheduler jobs delete trigger-gam-geo-monthly --location=europe-west1

# Les recréer
./create_schedulers.sh
```
