#!/usr/bin/env python3
import os
import glob
import csv
import shutil
import psycopg2
from psycopg2 import sql

PG_HOST = "localhost"
PG_PORT = 5432

PG_DB = "ERP_Database"
PG_USER = "nestor"
PG_PWD = "12ab12ab"
PG_SCHEMA = "public"

# directorios
DATA_PENDING = "../data_pending/"
DATA_PROCESSED = "../data_processed/"


def normalize_identifier(name: str) -> str:
    import re

    name = name.strip().lower()
    name = re.sub(r"[^a-z0-9_]", "_", name)
    name = re.sub(r"_+", "_", name).strip("_")
    if not name:
        name = "col"
    if name[0].isdigit():
        name = f"c_{name}"
    return name


def ensure_schema(conn, schema: str):
    with conn.cursor() as cur:
        cur.execute(
            sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(sql.Identifier(schema))
        )
    conn.commit()


def ensure_table(conn, table_name: str, headers):
    norm_cols = [normalize_identifier(h) for h in headers]
    with conn.cursor() as cur:
        columns = [sql.SQL("{} TEXT").format(sql.Identifier(c)) for c in norm_cols]
        q = sql.SQL("CREATE TABLE IF NOT EXISTS {}.{} ({})").format(
            sql.Identifier(PG_SCHEMA),
            sql.Identifier(table_name),
            sql.SQL(", ").join(columns),
        )
        cur.execute(q)
    conn.commit()


def load_csv_append(conn, table_name: str, csv_path: str):
    with open(csv_path, "r", encoding="utf-8", errors="replace") as f:
        reader = csv.reader(f)
        headers = next(reader)

        ensure_table(conn, table_name, headers)

        f.seek(0)
        with conn.cursor() as cur:
            copy_sql = sql.SQL("COPY {}.{} FROM STDIN WITH CSV HEADER").format(
                sql.Identifier(PG_SCHEMA), sql.Identifier(table_name)
            )
            cur.copy_expert(copy_sql, f)
    conn.commit()


def main():
    os.makedirs(DATA_PENDING, exist_ok=True)
    os.makedirs(DATA_PROCESSED, exist_ok=True)

    csv_files = sorted(glob.glob(os.path.join(DATA_PENDING, "*.csv")))
    if not csv_files:
        print("No hay CSV nuevos para cargar.")
        return

    conn = psycopg2.connect(
        host=PG_HOST, port=PG_PORT, dbname=PG_DB, user=PG_USER, password=PG_PWD
    )

    try:
        ensure_schema(conn, PG_SCHEMA)

        print(f"Procesando {len(csv_files)} CSVs nuevos...")
        for path in csv_files:
            base = os.path.basename(path)
            table = normalize_identifier(os.path.splitext(base)[0])
            print(f"-> Cargando incrementalmente {base} en {PG_SCHEMA}.{table} ...")

            load_csv_append(conn, table, path)

            # mover a processed
            shutil.move(path, os.path.join(DATA_PROCESSED, base))
            print(f"   Archivo movido a {DATA_PROCESSED}")

        print("Carga incremental completa.")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
