"""Audit complet des tables BigQuery pour vérifier la qualité des données."""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent / "cloud_function"))

from google.cloud import bigquery
from config import PROJECT_ID, DATASET_ID


def audit_table(client: bigquery.Client, table_name: str):
    """Audit complet d'une table."""
    print(f"\n{'=' * 80}")
    print(f"TABLE: {table_name}")
    print('=' * 80)

    table_id = f"{PROJECT_ID}.{DATASET_ID}.{table_name}"

    # 1. Schema
    print("\n1. SCHEMA:")
    schema_query = f"""
    SELECT column_name, data_type, is_nullable
    FROM `{PROJECT_ID}.{DATASET_ID}.INFORMATION_SCHEMA.COLUMNS`
    WHERE table_name = '{table_name}'
    ORDER BY ordinal_position
    """
    schema_result = client.query(schema_query).result()
    for row in schema_result:
        nullable = "NULL" if row.is_nullable == "YES" else "NOT NULL"
        print(f"  {row.column_name:35} {row.data_type:10} {nullable}")

    # 2. Nombre de lignes
    count_query = f"SELECT COUNT(*) as cnt FROM `{table_id}`"
    count_result = list(client.query(count_query).result())
    total_rows = count_result[0].cnt
    print(f"\n2. TOTAL ROWS: {total_rows:,}")

    # 3. Plage de dates
    print("\n3. PLAGE DE DATES:")
    if table_name in ["inventory_daily", "report_fill_rate"]:
        date_col = "date"
    else:
        date_col = "report_date"

    try:
        date_query = f"""
        SELECT
            MIN({date_col}) as min_date,
            MAX({date_col}) as max_date,
            COUNT(DISTINCT {date_col}) as unique_dates
        FROM `{table_id}`
        """
        date_result = list(client.query(date_query).result())
        if date_result:
            row = date_result[0]
            print(f"  Min date: {row.min_date}")
            print(f"  Max date: {row.max_date}")
            print(f"  Dates uniques: {row.unique_dates}")
    except Exception as e:
        print(f"  [SKIP] Pas de colonne de date standard: {e}")

    # 4. Vérification des NULL dans les colonnes importantes
    print("\n4. VERIFICATION NULL:")
    null_query = f"""
    SELECT
        COUNT(*) as total,
        COUNTIF({date_col} IS NULL) as null_dates
    FROM `{table_id}`
    """
    try:
        null_result = list(client.query(null_query).result())
        if null_result:
            row = null_result[0]
            if row.null_dates > 0:
                print(f"  [WARNING] {row.null_dates} lignes avec date NULL")
            else:
                print(f"  [OK] Aucune date NULL")
    except:
        pass

    # 5. Vérification des doublons
    print("\n5. VERIFICATION DOUBLONS:")
    if table_name == "inventory_daily":
        # Doublons = même date + ad_unit_name + order_name + device_category + creative_size
        dup_query = f"""
        SELECT
            date,
            ad_unit_name,
            order_name,
            device_category,
            creative_size,
            COUNT(*) as cnt
        FROM `{table_id}`
        GROUP BY date, ad_unit_name, order_name, device_category, creative_size
        HAVING COUNT(*) > 1
        LIMIT 5
        """
    elif table_name == "geo_monthly":
        # Doublons = même report_date + country_code
        dup_query = f"""
        SELECT
            report_date,
            country_code,
            COUNT(*) as cnt
        FROM `{table_id}`
        GROUP BY report_date, country_code
        HAVING COUNT(*) > 1
        LIMIT 5
        """
    elif table_name == "report_audience_interest":
        # Doublons = même report_date + interest_category
        dup_query = f"""
        SELECT
            report_date,
            interest_category,
            COUNT(*) as cnt
        FROM `{table_id}`
        GROUP BY report_date, interest_category
        HAVING COUNT(*) > 1
        LIMIT 5
        """
    elif table_name == "report_audience_demographics":
        # Doublons = même report_date + gender + age_bracket
        dup_query = f"""
        SELECT
            report_date,
            gender,
            age_bracket,
            COUNT(*) as cnt
        FROM `{table_id}`
        GROUP BY report_date, gender, age_bracket
        HAVING COUNT(*) > 1
        LIMIT 5
        """
    elif table_name == "report_fill_rate":
        # Doublons = même date + ad_unit_name
        dup_query = f"""
        SELECT
            date,
            ad_unit_name,
            COUNT(*) as cnt
        FROM `{table_id}`
        GROUP BY date, ad_unit_name
        HAVING COUNT(*) > 1
        LIMIT 5
        """
    else:
        dup_query = None

    if dup_query:
        dup_result = list(client.query(dup_query).result())
        if dup_result:
            print(f"  [WARNING] {len(dup_result)} groupes de doublons detectes")
            for row in dup_result[:3]:
                print(f"    {dict(row.items())}")
        else:
            print(f"  [OK] Aucun doublon detecte")

    # 6. Exemples de données
    print("\n6. EXEMPLES DE DONNEES (3 premières lignes):")
    sample_query = f"SELECT * FROM `{table_id}` LIMIT 3"
    sample_result = client.query(sample_query).result()
    for i, row in enumerate(sample_result, 1):
        print(f"  Ligne {i}: {dict(row.items())}")


def main():
    """Audit de toutes les tables."""
    print("=" * 80)
    print("AUDIT COMPLET DES TABLES BIGQUERY")
    print("=" * 80)

    client = bigquery.Client(project=PROJECT_ID)

    tables = [
        "inventory_daily",
        "geo_monthly",
        "report_audience_interest",
        "report_audience_demographics",
        "report_fill_rate",
    ]

    for table_name in tables:
        try:
            audit_table(client, table_name)
        except Exception as e:
            print(f"\n[ERROR] ERREUR lors de l'audit de {table_name}: {e}")
            import traceback
            traceback.print_exc()

    # Résumé final
    print(f"\n{'=' * 80}")
    print("RÉSUMÉ FINAL")
    print('=' * 80)

    for table_name in tables:
        count_query = f"SELECT COUNT(*) as cnt FROM `{PROJECT_ID}.{DATASET_ID}.{table_name}`"
        result = list(client.query(count_query).result())
        count = result[0].cnt
        print(f"  {table_name:35} {count:>10,} lignes")


if __name__ == "__main__":
    main()
