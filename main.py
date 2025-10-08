import os
import re
import io
import json
import tempfile
from datetime import datetime
from typing import List

import pandas as pd
from unidecode import unidecode
from google.cloud import storage, bigquery
from google.api_core.exceptions import NotFound

TABLE_NAME = "base_geral"
BQ_DATASET = os.environ.get("BQ_DATASET")
PROJECT_ID = os.environ.get("GCP_PROJECT") or os.environ.get("GOOGLE_CLOUD_PROJECT")


# --------- helpers ---------
def normalize_header(h: str) -> str:
    s = unidecode(str(h)).lower().strip()
    s = re.sub(r"[^\w]+", "_", s)
    s = re.sub(r"_+", "_", s).strip("_")
    if re.match(r"^\d", s):
        s = "_" + s
    return s or "col"

def to_str_or_none(v):
    if v is None:
        return None
    # pandas NaN/NaT
    try:
        import math
        if isinstance(v, float) and math.isnan(v):
            return None
    except Exception:
        pass
    # pandas NA
    try:
        import pandas as _pd  # local
        if _pd.isna(v):
            return None
    except Exception:
        pass
    s = str(v).strip()
    return s if s != "" else None

def ensure_dataset(bq: bigquery.Client, dataset_id: str):
    ds_ref = bigquery.Dataset(f"{PROJECT_ID}.{dataset_id}")
    try:
        bq.get_dataset(ds_ref)
    except NotFound:
        bq.create_dataset(ds_ref, exists_ok=True)
        print(f"Dataset criado: {dataset_id}")

def build_string_schema(cols: List[str]):
    return [bigquery.SchemaField(c, "STRING") for c in cols]

def entryPoint(data, context):
    # valida evento
    bucket = data.get("bucket")
    name = data.get("name")  # ex.: "pasta/base_geral.xlsx"
    if not bucket or not name:
        print("Evento sem bucket/name. Ignorando.")
        return

    base = os.path.basename(name).lower()
    if base != "base_geral.xlsx":
        print(f"Ignorando {name} (somente base_geral.xlsx).")
        return

    if not PROJECT_ID:
        raise RuntimeError("PROJECT_ID não encontrado no ambiente.")
    if not BQ_DATASET:
        raise RuntimeError("Defina a env var BQ_DATASET com o dataset do BigQuery.")

    table_id = f"{PROJECT_ID}.{BQ_DATASET}.{TABLE_NAME}"
    print(f"FULL LOAD → {table_id} a partir de gs://{bucket}/{name}")

    # --- baixa do GCS ---
    storage_client = storage.Client()
    blob = storage_client.bucket(bucket).blob(name)
    xlsx_path = os.path.join(tempfile.gettempdir(), "base_geral.xlsx")
    blob.download_to_filename(xlsx_path)
    print(f"Baixado: {xlsx_path}")

    # --- lê excel (tudo string depois) ---
    df = pd.read_excel(xlsx_path)  # deixa o pandas inferir; convertendo depois
    # remove linhas totalmente vazias
    df.dropna(how="all", inplace=True)

    # normaliza headers + dedup
    new_cols = []
    used = {}
    for col in df.columns:
        norm = normalize_header(col)
        if norm in used:
            used[norm] += 1
            norm = f"{norm}_{used[norm]}"
        else:
            used[norm] = 1
        new_cols.append(norm)
    df.columns = new_cols

    # converte TODAS as colunas para string (ou NULL)
    for c in df.columns:
        df[c] = df[c].map(to_str_or_none)

    # --- prepara JSONL em /tmp ---
    jsonl_path = os.path.join(tempfile.gettempdir(), "base_geral.jsonl")
    with open(jsonl_path, "w", encoding="utf-8") as f:
        for _, row in df.iterrows():
            obj = {c: (None if row[c] is None else str(row[c])) for c in df.columns}
            f.write(json.dumps(obj, ensure_ascii=False) + "\n")
    print(f"JSONL gerado: {jsonl_path}")

    # --- BigQuery: cria dataset (se faltar) e carrega com WRITE_TRUNCATE ---
    bq = bigquery.Client()
    ensure_dataset(bq, BQ_DATASET)

    # schema 100% STRING (com base nas colunas do arquivo)
    schema = build_string_schema(list(df.columns))

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,  # FULL LOAD
        create_disposition=bigquery.CreateDisposition.CREATE_IF_NEEDED,
        schema=schema,
        autodetect=False,
    )

    with open(jsonl_path, "rb") as f:
        job = bq.load_table_from_file(f, table_id, job_config=job_config)
    job.result()

    table = bq.get_table(table_id)
    print(f"FULL LOAD concluído: {table.num_rows} linhas em {table_id}")
    return
