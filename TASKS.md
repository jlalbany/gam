# Task List - Migration API REST + Nouveaux Rapports

## ✅ Complété

- [x] Recherche et validation API REST beta
- [x] Mise à jour requirements.txt (googleads → google-ads-admanager)
- [x] Création structure de base GAMRestClient
- [x] Documentation (.claude.md, MIGRATION.md, REPORTS_SPECS.md)

## 🔄 En Cours

### Phase 1: Infrastructure de Base
- [x] Créer classe `GAMRestClient`
- [x] Implémenter `create_and_run_report()` générique
- [x] Implémenter `_fetch_report_data()`
- [ ] **[NEXT]** Créer méthode `_transform_dataframe()` générique
- [ ] Tester client avec rapport simple (inventory 1 jour)

**Assigné à:** Agent Infrastructure
**Fichier:** `cloud_function/utils/gam_rest_client.py`
**Priorité:** HAUTE
**Durée estimée:** 30 min

---

## 📋 À Faire - Par Phase

### Phase 2A: Report Inventory + Active View

- [ ] Mettre à jour schéma BigQuery (ajouter colonnes Active View)
- [ ] Tester méthode `get_inventory_daily_report()` existante
- [ ] Valider mapping colonnes API REST
- [ ] Vérifier typage données (Int64, Date)
- [ ] Tester avec données 23 nov 2025
- [ ] Valider contre données actuelles (pas de régression)

**Assigné à:** Agent Report Inventory
**Fichiers:**
- `cloud_function/utils/gam_rest_client.py`
- `bigquery_schemas/report_inventory_daily.json`

**Priorité:** HAUTE
**Durée estimée:** 30 min
**Bloqué par:** Phase 1

---

### Phase 2B: Report Remplissage (Fill Rate)

- [ ] Vérifier disponibilité métriques (TOTAL_CODE_SERVES, TOTAL_AD_REQUESTS)
- [ ] Créer schéma BigQuery `report_fill_rate.json`
- [ ] Définir configuration rapport dans client
- [ ] Implémenter `get_fill_rate_report()` dans GAMRestClient
- [ ] Créer méthode dans BigQuery client
- [ ] Tester avec données 1 jour
- [ ] Valider calculs Fill Rate

**Assigné à:** Agent Report Fill Rate
**Fichiers:**
- `cloud_function/utils/gam_rest_client.py` (nouvelle méthode)
- `bigquery_schemas/report_fill_rate.json` (NOUVEAU)
- `cloud_function/utils/bigquery_client.py` (si modifications nécessaires)

**Priorité:** MOYENNE
**Durée estimée:** 45 min
**Bloqué par:** Phase 1

**Notes:**
- Vérifier noms exacts métriques dans API
- Confirmer fréquence (daily ou autre)

---

### Phase 2C: Report Audience - Centres d'Intérêt

⚠️ **ATTENTION: Tests requis avant implémentation**

- [ ] **TEST CRITIQUE:** Vérifier disponibilité dimension INTEREST
- [ ] **TEST CRITIQUE:** Créer rapport test pour novembre 2025
- [ ] **TEST CRITIQUE:** Identifier métriques GA réellement disponibles
- [ ] Si tests OK: Créer schéma BigQuery `report_audience_interest.json`
- [ ] Si tests OK: Implémenter `get_audience_interest_report()`
- [ ] Si tests KO: Documenter limitations et alternatives

**Assigné à:** Agent Report Audience Interest
**Fichiers:**
- Test d'abord (script Python standalone)
- `bigquery_schemas/report_audience_interest.json` (NOUVEAU si tests OK)
- `cloud_function/utils/gam_rest_client.py` (nouvelle méthode si tests OK)

**Priorité:** MOYENNE (mais tests HAUTE priorité)
**Durée estimée:** 1h (incluant tests)
**Bloqué par:** Phase 1

**Risques:**
- Dimension INTEREST peut ne pas avoir de données
- Métriques GA peuvent ne pas être disponibles via GAM API
- Peut nécessiter intégration GA4 séparée

---

### Phase 2D: Report Audience - Démographie

⚠️ **ATTENTION: Tests requis avant implémentation**

- [ ] **TEST CRITIQUE:** Vérifier GENDER et AGE_BRACKET séparés
- [ ] **TEST CRITIQUE:** Sinon vérifier GRP_DEMOGRAPHICS disponible
- [ ] **TEST CRITIQUE:** Créer rapport test pour novembre 2025
- [ ] **TEST CRITIQUE:** Vérifier si REACH report requis
- [ ] Si tests OK: Créer schéma BigQuery `report_audience_demo.json`
- [ ] Si tests OK: Implémenter `get_audience_demo_report()`
- [ ] Si GRP_DEMOGRAPHICS: Parser format MALE_18_24 → colonnes séparées

**Assigné à:** Agent Report Audience Demo
**Fichiers:**
- Test d'abord (script Python standalone)
- `bigquery_schemas/report_audience_demo.json` (NOUVEAU si tests OK)
- `cloud_function/utils/gam_rest_client.py` (nouvelle méthode si tests OK)

**Priorité:** MOYENNE (mais tests HAUTE priorité)
**Durée estimée:** 1h (incluant tests)
**Bloqué par:** Phase 1

**Risques:**
- Données démographiques peuvent être limitées (RGPD)
- Format dimension peut être différent (combiné vs séparé)
- Peut nécessiter type de rapport différent (REACH vs HISTORICAL)

---

### Phase 3: Intégration Cloud Function

- [ ] Remplacer import `from utils.gam_client import GAMReportClient`
- [ ] Par `from utils.gam_rest_client import GAMRestClient`
- [ ] Mettre à jour initialisation client dans `main.py`
- [ ] Ajouter handlers pour nouveaux rapports (fill_rate, audience_*)
- [ ] Mettre à jour configuration report types
- [ ] Tester localement avec Functions Framework
- [ ] Valider tous les rapports s'exécutent

**Assigné à:** Agent Integration
**Fichiers:**
- `cloud_function/main.py`
- `cloud_function/config.py` (si modifications nécessaires)

**Priorité:** HAUTE
**Durée estimée:** 30 min
**Bloqué par:** Phases 2A, 2B (au minimum)

**Notes:**
- Garder `gam_client.py` en backup
- Ajouter variable d'environnement pour switch si nécessaire

---

### Phase 4: Scripts Backfill

- [ ] Copier structure GAMRestClient dans backfill
- [ ] Implémenter fonction backfill inventory + Active View
- [ ] Implémenter fonction backfill fill rate
- [ ] Implémenter fonction backfill audience interest (si validé)
- [ ] Implémenter fonction backfill audience demo (si validé)
- [ ] Ajouter logique DELETE before INSERT
- [ ] Ajouter progress bar / logging
- [ ] Tester avec petit dataset (1 mois)

**Assigné à:** Agent Backfill
**Fichiers:**
- `backfill/backfill_local.py`
- Potentiellement créer `backfill/gam_rest_client.py` (copie)

**Priorité:** HAUTE
**Durée estimée:** 1h
**Bloqué par:** Phase 3

**Notes:**
- Ne pas modifier données existantes avant validation
- Créer tables de test d'abord (_test suffix)

---

### Phase 5: Schémas BigQuery

- [ ] Mettre à jour `report_inventory_daily.json` (Active View)
- [ ] Créer `report_fill_rate.json`
- [ ] Créer `report_audience_interest.json` (si validé)
- [ ] Créer `report_audience_demo.json` (si validé)
- [ ] Valider partitioning pour chaque table
- [ ] Créer script `setup_new_tables.sh`
- [ ] Tester création tables localement

**Assigné à:** Agent Schemas
**Fichiers:**
- `bigquery_schemas/*.json`
- `deployment/setup_new_tables.sh` (NOUVEAU)

**Priorité:** MOYENNE
**Durée estimée:** 30 min
**Bloqué par:** Phases 2*

**Notes:**
- Peut être fait en parallèle avec Phase 2
- Validation finale après tests

---

### Phase 6: Configuration Schedulers

- [ ] Mettre à jour scheduler inventory (si changements)
- [ ] Créer scheduler fill rate (quotidien 6h15?)
- [ ] Créer scheduler audience interest (mensuel 1er 7h30?)
- [ ] Créer scheduler audience demo (mensuel 1er 7h45?)
- [ ] Configurer OIDC authentication pour nouveaux schedulers
- [ ] Mettre à jour `create_schedulers.sh`
- [ ] Documenter dans README

**Assigné à:** Agent Schedulers
**Fichiers:**
- `deployment/create_schedulers.sh`

**Priorité:** MOYENNE
**Durée estimée:** 30 min
**Bloqué par:** Phase 3

**Notes:**
- Décaler horaires pour éviter concurrence
- Tester déclenchement manuel avant activation auto

---

### Phase 7: Déploiement & Validation

**Pré-déploiement:**
- [ ] Backup données actuelles
- [ ] Créer tables test avec suffix `_test`
- [ ] Valider tous les tests passent
- [ ] Review code complet
- [ ] Commit changements

**Déploiement:**
- [ ] Déployer nouvelle Cloud Function (test d'abord)
- [ ] Tester chaque rapport manuellement
- [ ] Vérifier logs Cloud Function
- [ ] Valider données dans BigQuery

**Migration données:**
- [ ] Vider tables existantes (inventory, geo)
- [ ] Lancer backfill complet 2024
- [ ] Lancer backfill complet 2025
- [ ] Vérifier absence doublons
- [ ] Valider totaux correspondent

**Post-déploiement:**
- [ ] Activer nouveaux schedulers
- [ ] Monitorer exécutions automatiques
- [ ] Vérifier Looker Studio affiche bonnes données
- [ ] Documenter changements
- [ ] Supprimer ancien code SOAP

**Assigné à:** Agent Deploy
**Priorité:** CRITIQUE
**Durée estimée:** 2h
**Bloqué par:** Toutes phases précédentes

---

## 🚨 Blockers & Risques

### Bloqueurs Potentiels

1. **Dimension INTEREST non disponible**
   - Impact: Report 4.1 impossible
   - Mitigation: Documenter, proposer alternatives

2. **Métriques GA non disponibles via GAM**
   - Impact: Reports 4.1 et 4.2 incomplets
   - Mitigation: Utiliser métriques GAM uniquement, ou intégrer GA4 API

3. **Démographie données insuffisantes**
   - Impact: Report 4.2 vide ou limité
   - Mitigation: Vérifier volume minimal requis

4. **Limites rate limiting API**
   - Impact: Backfill lent ou échoue
   - Mitigation: Implémenter retry + delays

### Risques Techniques

- **Breaking changes API REST:** Docs beta peuvent changer
- **Mapping colonnes différent:** Noms peuvent varier
- **Authentification ADC:** Peut nécessiter configuration spécifique
- **Timeout rapports longs:** Novembre 2025 peut être volumineux

---

## 📊 Métriques de Progression

**Overall Progress:** 15% (4/27 tâches principales)

### Par Phase
- Phase 1 (Infrastructure): 60% (3/5)
- Phase 2A (Inventory): 0% (0/6)
- Phase 2B (Fill Rate): 0% (0/7)
- Phase 2C (Interest): 0% (0/6)
- Phase 2D (Demo): 0% (0/7)
- Phase 3 (Integration): 0% (0/7)
- Phase 4 (Backfill): 0% (0/8)
- Phase 5 (Schemas): 0% (0/7)
- Phase 6 (Schedulers): 0% (0/7)
- Phase 7 (Deploy): 0% (0/15)

---

## 🎯 Prochaines Actions Immédiates

**PRIORITÉ 1 (Aujourd'hui):**
1. Terminer Phase 1 (méthode `_transform_dataframe()` générique)
2. Tester client REST avec rapport inventory 1 jour
3. Lancer tests disponibilité INTEREST et GENDER/AGE_BRACKET

**PRIORITÉ 2 (Après validation tests):**
4. Finaliser Phase 2A (inventory + Active View)
5. Implémenter Phase 2B (fill rate)
6. Décider sur Phases 2C/2D selon résultats tests

**PRIORITÉ 3 (Validation avant prod):**
7. Intégration Cloud Function (Phase 3)
8. Scripts backfill (Phase 4)
9. Tests complets end-to-end

---

## 📝 Notes de Session

_Mettre à jour après chaque session de travail_

**Session 2025-11-24:**
- ✅ Documentation complète créée
- ✅ Structure de base client REST
- ⏳ Tests dimensions GA requis avant continuer
- ⏳ Décision architecture finale selon résultats tests
