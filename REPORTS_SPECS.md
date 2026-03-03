# Spécifications des Rapports GAM



BQ_Demo : parent -  report_id=6493277740 
BQ_Interests : parent -  report_id=6493240984
BQ_ReportInventoryDaily : parent -  report_id=6493295630
BQ_ReportRemplissage : parent -  report_id=6493312298
BQ_Report Géographique : parent -  report_id=66493333640












## 1. Report Inventory Daily (MISE À JOUR)

### Objectif
Rapport quotidien des performances d'inventaire avec métriques de visibilité Active View.

### Type de Rapport
`HISTORICAL`

### Fréquence
Quotidien (6h Europe/Paris)

### Dimensions
```python
dimensions = [
    "DATE",                    # Date du rapport
    "AD_UNIT_ID",             # ID du bloc d'annonce
    "AD_UNIT_NAME",           # Nom du bloc d'annonce
    "ORDER_ID",               # ID de l'ordre publicitaire
    "ORDER_NAME",             # Nom de l'ordre
    "DEVICE_CATEGORY_NAME",   # Catégorie d'appareil (Desktop, Mobile, Tablet)
    "CREATIVE_SIZE",          # Taille de la créative (ex: 728x90)
]
```

### Métriques
```python
metrics = [
    "TOTAL_IMPRESSIONS",                              # Impressions totales
    "TOTAL_CLICKS",                                   # Clics totaux
    "TOTAL_ACTIVE_VIEW_MEASURABLE_IMPRESSIONS",      # Impressions mesurables Active View (NOUVEAU)
    "TOTAL_ACTIVE_VIEW_VIEWABLE_IMPRESSIONS",        # Impressions visibles Active View (NOUVEAU)
]
```

### Schéma BigQuery

**Table:** `report_inventory_daily`
**Partitioning:** Par `date` (DAY)

```json
[
  {"name": "date", "type": "DATE", "mode": "REQUIRED"},
  {"name": "ad_unit_id", "type": "INTEGER", "mode": "NULLABLE"},
  {"name": "ad_unit_name", "type": "STRING", "mode": "NULLABLE"},
  {"name": "order_id", "type": "INTEGER", "mode": "NULLABLE"},
  {"name": "order_name", "type": "STRING", "mode": "NULLABLE"},
  {"name": "device_category", "type": "STRING", "mode": "NULLABLE"},
  {"name": "creative_size", "type": "STRING", "mode": "NULLABLE"},
  {"name": "ad_server_impressions", "type": "INTEGER", "mode": "NULLABLE"},
  {"name": "ad_server_clicks", "type": "INTEGER", "mode": "NULLABLE"},
  {"name": "active_view_measurable_impressions", "type": "INTEGER", "mode": "NULLABLE"},
  {"name": "active_view_viewable_impressions", "type": "INTEGER", "mode": "NULLABLE"}
]
```

### Mapping Colonnes (API REST → BigQuery)

```python
column_mapping = {
    "Date": "date",
    "Ad unit ID": "ad_unit_id",
    "Ad unit": "ad_unit_name",
    "Order ID": "order_id",
    "Order": "order_name",
    "Device category": "device_category",
    "Creative size": "creative_size",
    "Total impressions": "ad_server_impressions",
    "Total clicks": "ad_server_clicks",
    "Total Active View measurable impressions": "active_view_measurable_impressions",
    "Total Active View viewable impressions": "active_view_viewable_impressions",
}
```

### Typage
- `date`: pd.to_datetime().dt.date
- `*_id`: pd.to_numeric().astype("Int64")
- `*_impressions`, `*_clicks`: pd.to_numeric().astype("Int64")
- Autres: STRING (défaut)

### Exemple de Donnée
```
date: 2025-11-23
ad_unit_id: 22312747258
ad_unit_name: data.fei.org
order_id: 2819668036
order_name: 2102_FEI_HouseAds
device_category: Desktop
creative_size: 728 x 90
ad_server_impressions: 5796
ad_server_clicks: 10
active_view_measurable_impressions: 5234
active_view_viewable_impressions: 4821
```

---

## 2. Report Remplissage (NOUVEAU)

### Objectif
Taux de remplissage des blocs d'annonce (Fill Rate).

**Calcul:** `Fill Rate = Total Impressions / Total Ad Requests`

### Type de Rapport
`HISTORICAL`

### Fréquence
Quotidien (même temps que inventory)

### Dimensions
```python
dimensions = [
    "DATE",              # Date du rapport
    "AD_UNIT_ID",       # ID du bloc d'annonce
    "AD_UNIT_NAME",    # Nom du bloc d'annonce
    "CREATIVE_SIZE",   # Taille de la créative (ex: 728x90)
]
```

### Métriques
```python
metrics = [
    "TOTAL_CODE_SERVES",      # Nombre total de fois que le code d'annonce a été servi
    "TOTAL_AD_REQUESTS",      # Nombre total de demandes d'annonces
    "TOTAL_IMPRESSIONS",      # Nombre total d'impressions
]
```

### Schéma BigQuery

**Table:** `report_fill_rate`
**Partitioning:** Par `date` (DAY)

```json
[
  {"name": "date", "type": "DATE", "mode": "REQUIRED"},
  {"name": "ad_unit_id", "type": "INTEGER", "mode": "NULLABLE"},
  {"name": "ad_unit_name", "type": "STRING", "mode": "NULLABLE"},
  {"name": "total_code_serves", "type": "INTEGER", "mode": "NULLABLE"},
  {"name": "total_ad_requests", "type": "INTEGER", "mode": "NULLABLE"},
  {"name": "total_impressions", "type": "INTEGER", "mode": "NULLABLE"}
]
```

### Mapping Colonnes

```python
column_mapping = {
    "Date": "date",
    "Ad unit ID": "ad_unit_id",
    "Ad unit": "ad_unit_name",
    "Total code serves": "total_code_serves",
    "Total ad requests": "total_ad_requests",
    "Total impressions": "total_impressions",
}
```

### Calculs Dérivés (Dans Looker Studio)
```sql
fill_rate_percent = (total_impressions / total_ad_requests) * 100
serve_rate_percent = (total_code_serves / total_ad_requests) * 100
```

### Exemple de Donnée
```
date: 2025-11-23
ad_unit_id: 22312747258
ad_unit_name: data.fei.org
total_code_serves: 125000
total_ad_requests: 150000
total_impressions: 98000
# Fill Rate = 98000/150000 = 65.33%
```

---

## 3. Report Audience - Centres d'Intérêt (NOUVEAU)

### Objectif
Performance des annonces par centre d'intérêt utilisateur (données Google Analytics).

### Type de Rapport
`HISTORICAL`

### Fréquence
Mensuel (backfill novembre 2025 uniquement pour le moment)

### Dimensions
```python
dimensions = [
    "DATE",               # Date (pour agrégation mensuelle)
    "INTEREST",          # Centre d'intérêt Google Analytics
]
```

### Métriques (À VALIDER)
```python
metrics = [
    "TOTAL_IMPRESSIONS",        # Impressions
    "TOTAL_CLICKS",             # Clics
    # Métriques GA si disponibles:
    # "GA_SESSIONS",           # Sessions (si disponible)
    # "GA_USERS",              # Utilisateurs (si disponible)
    # "GA_PAGEVIEWS",          # Vues de page (si disponible)
]
```

### Schéma BigQuery

**Table:** `report_audience_interest`
**Partitioning:** Par `report_date` (DAY) - premier jour du mois

```json
[
  {"name": "report_date", "type": "DATE", "mode": "REQUIRED"},
  {"name": "interest", "type": "STRING", "mode": "NULLABLE"},
  {"name": "total_impressions", "type": "INTEGER", "mode": "NULLABLE"},
  {"name": "total_clicks", "type": "INTEGER", "mode": "NULLABLE"},
  {"name": "users_with_ad_impression", "type": "INTEGER", "mode": "NULLABLE"},
  {"name": "ga_sessions", "type": "INTEGER", "mode": "NULLABLE"},
  {"name": "ga_pageviews", "type": "INTEGER", "mode": "NULLABLE"}
]
```

### Mapping Colonnes (À AJUSTER SELON API)

```python
column_mapping = {
    "Interest": "interest",
    "Total impressions": "total_impressions",
    "Total clicks": "total_clicks",
    # À compléter selon métriques disponibles
}
```

### Notes Importantes
⚠️ **ATTENTION:** La dimension `INTEREST` nécessite:
1. Intégration Google Analytics avec GAM activée
2. Données utilisateur suffisantes
3. Vérifier disponibilité réelle dans l'API

**Test requis:** Créer un rapport test avec `INTEREST` pour vérifier données disponibles.

### Exemple de Donnée
```
report_date: 2025-11-01
interest: Sports
total_impressions: 125000
total_clicks: 450
users_with_ad_impression: 15000
ga_sessions: 18500
ga_pageviews: 45000
```

---

## 4. Report Audience - Démographie (NOUVEAU)

### Objectif
Performance des annonces par sexe et tranche d'âge.

### Type de Rapport
`HISTORICAL` ou `REACH` (à déterminer selon disponibilité données)

### Fréquence
Mensuel (backfill novembre 2025 uniquement pour le moment)

### Dimensions
```python
dimensions = [
    "DATE",              # Date (pour agrégation mensuelle)
    "GENDER",            # Sexe (Male, Female, Unknown)
    "AGE_BRACKET",       # Tranche d'âge (18-24, 25-34, etc.)
]
```

### Alternative si GENDER/AGE_BRACKET séparés non disponibles
```python
dimensions = [
    "DATE",
    "GRP_DEMOGRAPHICS",  # Combinaison sexe + âge (ex: MALE_18_24)
]
```

### Métriques
```python
metrics = [
    "TOTAL_IMPRESSIONS",        # Impressions
    "TOTAL_CLICKS",             # Clics
    "UNIQUE_REACH",             # Portée unique (si rapport REACH)
]
```

### Schéma BigQuery

**Table:** `report_audience_demo`
**Partitioning:** Par `report_date` (DAY) - premier jour du mois

```json
[
  {"name": "report_date", "type": "DATE", "mode": "REQUIRED"},
  {"name": "gender", "type": "STRING", "mode": "NULLABLE"},
  {"name": "age_bracket", "type": "STRING", "mode": "NULLABLE"},
  {"name": "total_impressions", "type": "INTEGER", "mode": "NULLABLE"},
  {"name": "total_clicks", "type": "INTEGER", "mode": "NULLABLE"},
  {"name": "users_with_ad_impression", "type": "INTEGER", "mode": "NULLABLE"},
  {"name": "ga_sessions", "type": "INTEGER", "mode": "NULLABLE"},
  {"name": "ga_pageviews", "type": "INTEGER", "mode": "NULLABLE"}
]
```

### Mapping Colonnes

```python
column_mapping = {
    "Gender": "gender",
    "Age bracket": "age_bracket",
    "Total impressions": "total_impressions",
    "Total clicks": "total_clicks",
}
```

### Valeurs Attendues

**Gender:**
- Male
- Female
- Unknown

**Age Bracket:**
- 18-24
- 25-34
- 35-44
- 45-54
- 55-64
- 65+
- Unknown

### Notes Importantes
⚠️ **ATTENTION:** Les données démographiques peuvent nécessiter:
1. Volume de trafic suffisant
2. Consentement utilisateurs (RGPD)
3. Configuration spécifique dans GAM

**Test requis:** Vérifier si données disponibles pour réseau 33047445.

### Exemple de Donnée
```
report_date: 2025-11-01
gender: Male
age_bracket: 25-34
total_impressions: 85000
total_clicks: 320
users_with_ad_impression: 12000
ga_sessions: 14500
ga_pageviews: 38000
```

---

## 5. Report Géographique (EXISTANT - Pas de changement)

### Objectif
Performance par pays.

### Type de Rapport
`HISTORICAL`

### Fréquence
Mensuel (1er du mois, 7h Europe/Paris)

### Dimensions
```python
dimensions = [
    "COUNTRY_CRITERIA_ID",    # ID critère pays
    "COUNTRY_NAME",           # Nom du pays
]
```

### Métriques
```python
metrics = [
    "TOTAL_IMPRESSIONS",
    "TOTAL_CLICKS",
]
```

### Schéma BigQuery
```json
[
  {"name": "report_date", "type": "DATE", "mode": "REQUIRED"},
  {"name": "country_id", "type": "INTEGER", "mode": "NULLABLE"},
  {"name": "country_name", "type": "STRING", "mode": "NULLABLE"},
  {"name": "ad_server_impressions", "type": "INTEGER", "mode": "NULLABLE"},
  {"name": "ad_server_clicks", "type": "INTEGER", "mode": "NULLABLE"}
]
```

---

## Tests Requis Avant Déploiement

### Pour chaque rapport:
1. ✅ Vérifier disponibilité dimensions dans API
2. ✅ Créer rapport test avec 1 jour de données
3. ✅ Vérifier noms de colonnes retournées
4. ✅ Valider types de données
5. ✅ Tester transformation DataFrame
6. ✅ Vérifier insertion BigQuery

### Cas particuliers:
- **INTEREST:** Vérifier si données GA disponibles
- **GENDER/AGE_BRACKET:** Tester si dimensions séparées ou combinées
- **Active View:** Vérifier si toutes les impressions ont métriques AV

## Documentation API Référence

- Dimensions: https://developers.google.com/ad-manager/api/beta/reference/rest/v1/networks.reports#dimension
- Métriques: https://developers.google.com/ad-manager/api/beta/reference/rest/v1/networks.reports#metric
- Types de rapports: https://developers.google.com/ad-manager/api/beta/reference/rest/v1/networks.reports#reporttype
