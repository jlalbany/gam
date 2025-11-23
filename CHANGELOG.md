# Changelog

Toutes les modifications notables de ce projet seront documentées dans ce fichier.

## [1.0.0] - 2025-11-23

### Ajouté

#### Infrastructure
- Configuration complète BigQuery avec 2 tables partitionnées
  - `report_inventory_daily` (partitionnée par date)
  - `report_geo_monthly` (partitionnée par report_date)
- Cloud Function Gen 2 avec router intelligent
- Cloud Scheduler pour automatisation quotidienne et mensuelle
- Secret Manager pour gestion sécurisée des credentials

#### Scripts
- Script de backfill pour récupération historique
  - Chunking par mois pour éviter les timeouts
  - Support multi-rapports (inventory, geo, all)
  - Gestion d'erreurs robuste
- Scripts de déploiement automatisés
  - `setup_all.sh` - Déploiement complet
  - `setup_bigquery.sh` - Configuration BigQuery
  - `deploy.sh` - Déploiement Cloud Function
  - `create_schedulers.sh` - Configuration schedulers

#### Modules
- `gam_client.py` - Client API GAM avec retry et timeout
- `bigquery_client.py` - Client BigQuery avec gestion de schémas
- `logger.py` - Logging structuré JSON pour Cloud Logging

#### Rapports
- **Rapport Inventaire Quotidien** (INVENTORY_DAILY)
  - Date range: YESTERDAY
  - Dimensions: Date, Ad Unit, Order, Device, Creative Size
  - Métriques: Impressions, Clics

- **Rapport Géo Mensuel** (GEO_MONTHLY)
  - Date range: LAST_MONTH
  - Dimensions: Country
  - Métriques: Impressions, Clics
  - Report date calculé automatiquement (1er du mois)

#### Documentation
- README principal complet
- Documentation backfill
- Documentation déploiement
- Exemples de requêtes SQL
- Guide de dépannage

### Caractéristiques

- 🔒 Sécurité : Credentials dans Secret Manager
- 📊 Logging structuré JSON pour monitoring
- ⚡ Timeout optimisé (540s) pour rapports volumineux
- 🔄 Stratégie de backfill avec chunking mensuel
- 🎯 Router intelligent pour multi-rapports
- 📅 Partitionnement BigQuery pour performances optimales
- 🚀 Déploiement automatisé one-click

### Architecture

- **Runtime** : Python 3.11
- **Infrastructure** : Google Cloud Functions Gen 2
- **Orchestration** : Google Cloud Scheduler
- **Database** : Google BigQuery (partitionné)
- **Secrets** : Google Secret Manager
- **Monitoring** : Google Cloud Logging (JSON structuré)

### Configuration Scheduler

- **Daily** : `0 6 * * *` (6h00 tous les jours)
- **Monthly** : `0 7 1 * *` (7h00 le 1er de chaque mois)
- **Timezone** : Europe/Paris
- **Auth** : OIDC avec Service Account

### Ressources

- **Memory** : 1GB
- **Timeout** : 540 secondes
- **Region** : europe-west1 (configurable)

## [Unreleased]

### Prévu

- Support pour rapports additionnels (Line Items, Creatives, etc.)
- Notifications par email en cas d'échec
- Dashboard Data Studio
- Tests unitaires et d'intégration
- CI/CD avec Cloud Build
