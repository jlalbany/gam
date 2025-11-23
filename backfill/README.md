# GAM Backfill Script

Ce script permet de récupérer l'historique des données GAM et de les charger dans BigQuery.

## Prérequis

1. Authentification GCP configurée :
   ```bash
   gcloud auth application-default login
   ```

2. Permissions requises :
   - Accès au Secret Manager (Secret Manager Secret Accessor)
   - Permissions d'écriture BigQuery
   - Les tables BigQuery doivent déjà exister

3. Installation des dépendances :
   ```bash
   pip install -r requirements.txt
   ```

## Utilisation

### Backfill complet (tous les rapports)

```bash
python backfill_gam_reports.py \
  --project-id YOUR_PROJECT_ID \
  --start-date 2023-01-01 \
  --end-date 2024-02-29
```

### Backfill d'un rapport spécifique

Inventaire uniquement :
```bash
python backfill_gam_reports.py \
  --project-id YOUR_PROJECT_ID \
  --start-date 2023-01-01 \
  --end-date 2024-02-29 \
  --reports inventory
```

Géo uniquement :
```bash
python backfill_gam_reports.py \
  --project-id YOUR_PROJECT_ID \
  --start-date 2023-01-01 \
  --end-date 2024-02-29 \
  --reports geo
```

### Options avancées

```bash
python backfill_gam_reports.py \
  --project-id YOUR_PROJECT_ID \
  --start-date 2023-01-01 \
  --end-date 2024-02-29 \
  --secret-name custom_secret_name \
  --dataset-id custom_dataset \
  --reports inventory geo
```

## Fonctionnement

Le script fonctionne en mode "chunking" pour éviter les problèmes de mémoire :

1. **Division par mois** : Au lieu de demander toute la période en une seule fois, le script découpe la période en mois
2. **Traitement séquentiel** : Chaque mois est traité indépendamment (fetch → transform → load)
3. **Logs détaillés** : Affichage de la progression dans la console

## Gestion des erreurs

- En cas d'erreur sur un mois, le script s'arrête et affiche l'erreur
- Les mois déjà traités restent en base (pas de rollback)
- Vous pouvez relancer le script avec une période ajustée pour compléter les données manquantes

## Durée d'exécution

Le temps d'exécution dépend de :
- Volume de données
- Performance de l'API GAM (génération des rapports)
- Période couverte

**Estimation** : Comptez ~2-5 minutes par mois pour le rapport inventaire.
