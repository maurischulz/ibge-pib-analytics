"""Carrega dados IBGE (reais ou simulados) no PostgreSQL local."""

from __future__ import annotations

import os
from pathlib import Path
from typing import Iterable, Sequence

import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv

from ibge_api_client import IBGEAPIClient, save_csv


def get_conn_params() -> dict:
    load_dotenv()
    return {
        "host": os.getenv("PG_IBGE_HOST", "localhost"),
        "port": int(os.getenv("PG_IBGE_PORT", "5433")),
        "dbname": os.getenv("PG_IBGE_DB", "ibge_data"),
        "user": os.getenv("PG_IBGE_USER", "ibge_user"),
        "password": os.getenv("PG_IBGE_PASSWORD", "ibge_password"),
    }


def create_raw_structures(conn: psycopg2.extensions.connection) -> None:
    with conn.cursor() as cur:
        cur.execute("create schema if not exists raw_ibge;")
        cur.execute(
            """
            create table if not exists raw_ibge.raw_pib_paises (
                pais text,
                trimestre integer,
                ano integer,
                pib numeric(18, 2),
                pib_per_capita numeric(12, 2)
            );
            """
        )
        cur.execute(
            """
            create table if not exists raw_ibge.raw_indicadores_paises (
                pais_id text,
                pais text,
                ano integer,
                indicador_id integer,
                indicador text,
                unidade text,
                valor numeric(18, 4)
            );
            """
        )
        cur.execute(
            """
            create table if not exists raw_ibge.raw_ipca (
                periodo text,
                valor numeric(6, 2)
            );
            """
        )
        cur.execute("truncate table raw_ibge.raw_pib_paises;")
        cur.execute("truncate table raw_ibge.raw_indicadores_paises;")
        cur.execute("truncate table raw_ibge.raw_ipca;")
    conn.commit()


def normalize(df: pd.DataFrame, columns: Sequence[str]) -> Iterable[tuple]:
    data = df.loc[:, columns].copy()
    data = data.where(pd.notnull(data), None)
    return [tuple(row) for row in data.itertuples(index=False, name=None)]


def insert_dataframe(
    conn: psycopg2.extensions.connection,
    table: str,
    columns: Sequence[str],
    rows: Iterable[tuple],
) -> None:
    with conn.cursor() as cur:
        execute_values(
            cur,
            f"insert into {table} ({', '.join(columns)}) values %s",
            list(rows),
            page_size=1000,
        )
    conn.commit()


def main() -> None:
    client = IBGEAPIClient(force_simulation=False)
    datasets = client.extract_all()

    Path("data/raw").mkdir(parents=True, exist_ok=True)
    for filename, df in datasets.items():
        save_csv(df, filename)

    pib_df = pd.read_csv("data/raw/ibge_pib_paises.csv")
    indicadores_df = pd.read_csv("data/raw/ibge_indicadores_paises.csv")
    ipca_df = pd.read_csv("data/raw/ibge_ipca.csv")

    conn = psycopg2.connect(**get_conn_params())
    try:
        create_raw_structures(conn)

        insert_dataframe(
            conn,
            "raw_ibge.raw_pib_paises",
            ["pais", "trimestre", "ano", "pib", "pib_per_capita"],
            normalize(pib_df, ["pais", "trimestre", "ano", "pib", "pib_per_capita"]),
        )
        insert_dataframe(
            conn,
            "raw_ibge.raw_indicadores_paises",
            ["pais_id", "pais", "ano", "indicador_id", "indicador", "unidade", "valor"],
            normalize(
                indicadores_df,
                ["pais_id", "pais", "ano", "indicador_id", "indicador", "unidade", "valor"],
            ),
        )
        insert_dataframe(
            conn,
            "raw_ibge.raw_ipca",
            ["periodo", "valor"],
            normalize(ipca_df, ["periodo", "valor"]),
        )

        print(f"[OK] raw_pib_paises: {len(pib_df)} linhas")
        print(f"[OK] raw_indicadores_paises: {len(indicadores_df)} linhas")
        print(f"[OK] raw_ipca: {len(ipca_df)} linhas")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
