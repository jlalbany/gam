# Plan de Migration: API SOAP → API REST

## Objectifs

1. Migrer de `googleads` (SOAP) vers `google-ads-admanager` (REST Beta)
2. Ajouter métriques Active View au rapport inventory
3. Créer 3 nouveaux rapports
4. Maintenir compatibilité données existantes
5. Éviter redondance de code

## Architecture Optimisée

### Principe: Configuration > Code

Au lieu de créer une méthode par rapport, on crée une méthode générique configurable.

### Structure du Client REST

```python
class GAMRestClient:
    def create_and_run_report(dimensions, metrics, start_date, end_date)
        # Méthode générique pour tous les rapports

    def _fetch_report_data(report_name)
        # Récupération des données (partagée)

    def _transform_dataframe(df, column_mapping, type_mapping)
        # Transformation générique (partagée)
```

### Configuration par Rapport

Chaque rapport = configuration JSON:

```python
REPORT_CONFIGS = {
    "inventory": {
        "dimensions": ["DATE", "AD_UNIT_NAME", ...],
        "metrics": ["TOTAL_IMPRESSIONS", ...],
        "column_mapping": {...},
        "type_mapping": {...}
    },
    "fill_rate": {...},
    "audience_interest": {...},
    "audience_demo": {...}
}
```

## Plan de Travail

### Phase 1: Infrastructure (Agent 1)
**Fichiers:** `gam_rest_client.py`, `requirements.txt`

Tâches:
1. ✅ Créer classe `GAMRestClient` de base
2. ✅ Implémenter méthode générique `create_and_run_report()`
3. ✅ Implémenter `_fetch_report_data()`
4. ⬜ Créer méthode `_transform_dataframe()` générique
5. ⬜ Tester avec rapport simple

### Phase 2: Rapports Individuels (Agents 2-5)

#### Agent 2: Report Inventory + Active View
**Fichier:** `gam_rest_client.py` (méthode `get_inventory_daily_report`)

Tâches:
1. ⬜ Définir configuration rapport inventory
2. ⬜ Ajouter métriques Active View
3. ⬜ Mettre à jour schéma BigQuery
4. ⬜ Tester transformation données
5. ⬜ Valider typage colonnes

#### Agent 3: Report Remplissage
**Fichiers:** `gam_rest_client.py` (nouvelle méthode), schéma BigQuery

Tâches:
1. ⬜ Créer configuration rapport remplissage
2. ⬜ Définir schéma BigQuery `report_fill_rate`
3. ⬜ Implémenter `get_fill_rate_report()`
4. ⬜ Tester avec données réelles
5. ⬜ Documenter mapping colonnes

#### Agent 4: Report Audience - Centres d'Intérêt
**Fichiers:** `gam_rest_client.py`, schéma BigQuery

Tâches:
1. ⬜ Vérifier disponibilité dimension INTEREST
2. ⬜ Créer configuration rapport
3. ⬜ Définir schéma BigQuery `report_audience_interest`
4. ⬜ Implémenter `get_audience_interest_report()`
5. ⬜ Tester novembre 2025

#### Agent 5: Report Audience - Démographie
**Fichiers:** `gam_rest_client.py`, schéma BigQuery

Tâches:
1. ⬜ Vérifier dimensions GENDER, AGE_BRACKET
2. ⬜ Créer configuration rapport
3. ⬜ Définir schéma BigQuery `report_audience_demo`
4. ⬜ Implémenter `get_audience_demo_report()`
5. ⬜ Tester novembre 2025

### Phase 3: Intégration Cloud Function (Agent 6)
**Fichier:** `main.py`

Tâches:
1. ⬜ Remplacer `GAMReportClient` par `GAMRestClient`
2. ⬜ Mettre à jour import
3. ⬜ Ajouter handlers nouveaux rapports
4. ⬜ Mettre à jour configuration
5. ⬜ Tester localement avec Functions Framework

### Phase 4: Scripts Backfill (Agent 7)
**Fichier:** `backfill/backfill_local.py`

Tâches:
1. ⬜ Copier logique client REST
2. ⬜ Ajouter fonctions 3 nouveaux rapports
3. ⬜ Implémenter logique DELETE before INSERT
4. ⬜ Ajouter progress tracking
5. ⬜ Tester avec petit dataset

### Phase 5: Schémas BigQuery (Agent 8)
**Dossier:** `bigquery_schemas/`

Tâches:
1. ⬜ Mettre à jour `report_inventory_daily.json` (Active View)
2. ⬜ Créer `report_fill_rate.json`
3. ⬜ Créer `report_audience_interest.json`
4. ⬜ Créer `report_audience_demo.json`
5. ⬜ Valider partitioning strategies

### Phase 6: Configuration Schedulers (Agent 9)
**Fichier:** `deployment/create_schedulers.sh`

Tâches:
1. ⬜ Ajouter scheduler fill rate (quotidien?)
2. ⬜ Ajouter scheduler audience interest (mensuel?)
3. ⬜ Ajouter scheduler audience demo (mensuel?)
4. ⬜ Configurer OIDC authentication
5. ⬜ Tester déclenchement manuel

### Phase 7: Déploiement & Tests (Agent 10)
**Fichiers:** Tous

Tâches:
1. ⬜ Vider tables existantes
2. ⬜ Déployer nouvelle Cloud Function
3. ⬜ Créer nouvelles tables BigQuery
4. ⬜ Tester chaque scheduler manuellement
5. ⬜ Lancer backfill complet
6. ⬜ Vérifier données dans Looker Studio

## Métriques de Succès

### Technique
- ⬜ 0 doublons dans les données
- ⬜ Tous les rapports s'exécutent sans erreur
- ⬜ Schedulers déclenchés automatiquement
- ⬜ Temps d'exécution < 5 min par rapport

### Données
- ⬜ Inventory: ~105K lignes (2024-2025)
- ⬜ Geo: ~4.7K lignes (2024-2025)
- ⬜ Fill rate: TBD
- ⬜ Audience interest: Novembre 2025 uniquement
- ⬜ Audience demo: Novembre 2025 uniquement

### Qualité Code
- ⬜ < 20% code dupliqué
- ⬜ Tous rapports utilisent méthode générique
- ⬜ Configuration centralisée
- ⬜ Documentation à jour

## Points d'Attention

### Mapping Colonnes
L'API REST retourne des noms en clair ("Total impressions") vs SOAP qui retourne avec préfixe ("Column.TOTAL_IMPRESSIONS").

**Solution:** Dictionnaires de mapping par rapport.

### Types de Données
BigQuery strict sur les types. S'assurer:
- Dates → DATE
- IDs → INT64
- Metrics → INT64
- Strings → STRING

### Authentification
L'API REST utilise ADC (Application Default Credentials) au lieu du fichier YAML.

**Solution:** Extraire clé JSON du Secret Manager et la placer dans `/tmp/` avec variable d'environnement.

### Limites API
- Max 10 rapports concurrent
- Timeout 10 minutes par rapport
- Rate limiting non documenté

**Solution:** Implémenter retry logic et sequential execution.

## Rollback Plan

Si problèmes critiques après déploiement:

1. Revenir à version précédente Cloud Function
2. Restaurer ancien client SOAP
3. Re-backfill avec ancienne API
4. Investiguer en local

**Backup:** Garder `gam_client.py` (SOAP) jusqu'à validation complète.

## Timeline Estimé

- Phase 1: 30 min ✅
- Phase 2: 2h (30 min par rapport x 4)
- Phase 3: 30 min
- Phase 4: 1h
- Phase 5: 30 min
- Phase 6: 30 min
- Phase 7: 2h (backfill inclus)

**Total:** ~7h de travail

## Dépendances Entre Tâches

```
Phase 1 (Infrastructure)
    ↓
Phase 2 (Rapports) - Parallélisable
    ↓
Phase 5 (Schémas) - Peut être parallèle avec Phase 2
    ↓
Phase 3 (Cloud Function)
    ↓
Phase 4 (Backfill)
    ↓
Phase 6 (Schedulers)
    ↓
Phase 7 (Déploiement)
```

## Notes pour Agents

- **Réutiliser** le code existant au maximum
- **Tester** chaque composant isolément avant intégration
- **Documenter** les mappings de colonnes découverts
- **Valider** les types de données avec `pd.info()`
- **Communiquer** les blockers via MIGRATION_LOG.md
