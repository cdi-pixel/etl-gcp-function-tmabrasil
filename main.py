import os
import re
import io
import json
import time
import tempfile
from typing import List
from datetime import datetime

import pandas as pd
from unidecode import unidecode
from google.cloud import storage, bigquery
from google.api_core.exceptions import NotFound

# ===================== CONFIG =====================
TABLE_NAME_BASE_GERAL   = "base_geral"
TABLE_NAME_QGC          = "qgc"
STATUS_TABLE_NAME       = "status_implantacao"   # <- tabela de status

# hardcoded conforme seu ambiente
BQ_DATASET = "tmabrasil"
PROJECT_ID = "tmabrasil"

# ===================== HELPERS =====================
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
    try:
        import pandas as _pd
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

def normalize_headers_df(df: pd.DataFrame) -> pd.DataFrame:
    new_cols, used = [], {}
    for col in df.columns:
        norm = normalize_header(col)
        if norm in used:
            used[norm] += 1
            norm = f"{norm}_{used[norm]}"
        else:
            used[norm] = 1
        new_cols.append(norm)
    df.columns = new_cols
    return df

def df_to_jsonl(df: pd.DataFrame, jsonl_path: str):
    with open(jsonl_path, "w", encoding="utf-8") as f:
        for _, row in df.iterrows():
            obj = {c: (None if row[c] is None else str(row[c])) for c in df.columns}
            f.write(json.dumps(obj, ensure_ascii=False) + "\n")
    return jsonl_path

# -------- status table (logs) --------
def ensure_status_table(bq: bigquery.Client):
    table_id = f"{PROJECT_ID}.{BQ_DATASET}.{STATUS_TABLE_NAME}"
    try:
        bq.get_table(table_id)
        return
    except NotFound:
        schema = [
            bigquery.SchemaField("data_envio", "TIMESTAMP"),
            bigquery.SchemaField("nomearquivo", "STRING"),
            bigquery.SchemaField("status", "STRING"),
            bigquery.SchemaField("mensagem", "STRING"),
        ]
        table = bigquery.Table(table_id, schema=schema)
        bq.create_table(table)
        print("[status] Tabela de status criada.")

def log_status(bq: bigquery.Client, nome_arquivo: str, status: str, mensagem: str):
    """Insere 1 linha na tabela de status. Não quebra o fluxo se falhar."""
    try:
        table_id = f"{PROJECT_ID}.{BQ_DATASET}.{STATUS_TABLE_NAME}"
        rows = [{
            "data_envio": datetime.utcnow().isoformat(" "),
            "nomearquivo": nome_arquivo,
            "status": status[:50],
            "mensagem": (mensagem or "")[:1500],
        }]
        bq.insert_rows_json(table_id, rows)
        print(f"[status] {status} | {nome_arquivo} | {mensagem}")
    except Exception as e:
        print(f"[status] Falha ao registrar status: {e}")

# ===================== FLUXO 1: base_geral.xlsx (FULL LOAD) =====================
def process_base_geral(bq: bigquery.Client, gcs_bucket: str, gcs_object: str):
    table_id = f"{PROJECT_ID}.{BQ_DATASET}.{TABLE_NAME_BASE_GERAL}"
    print(f"[base_geral] FULL LOAD → {table_id} a partir de gs://{gcs_bucket}/{gcs_object}")

    # baixa
    storage_client = storage.Client()
    blob = storage_client.bucket(gcs_bucket).blob(gcs_object)
    xlsx_path = os.path.join(tempfile.gettempdir(), "base_geral.xlsx")
    blob.download_to_filename(xlsx_path)

    # lê e normaliza
    df = pd.read_excel(xlsx_path)
    df.dropna(how="all", inplace=True)
    df = normalize_headers_df(df)
    for c in df.columns:
        df[c] = df[c].map(to_str_or_none)

    # JSONL + LOAD (WRITE_TRUNCATE, CREATE_IF_NEEDED cria a tabela se faltar)
    jsonl_path = os.path.join(tempfile.gettempdir(), "base_geral.jsonl")
    df_to_jsonl(df, jsonl_path)

    schema = build_string_schema(list(df.columns))
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        create_disposition=bigquery.CreateDisposition.CREATE_IF_NEEDED,
        schema=schema,
        autodetect=False,
    )
    with open(jsonl_path, "rb") as f:
        job = bq.load_table_from_file(f, table_id, job_config=job_config)
    job.result()

    table = bq.get_table(table_id)
    print(f"[base_geral] FULL LOAD concluído: {table.num_rows} linhas")

# ===================== FLUXO 2: qualquer outro .xlsx (INCREMENTAL QGC) =====================
QGC_BASE_COLUMNS  = ["grupo","empresa","fonte","classe","subclasse","credor","moeda","valor","data"]
QGC_FINAL_COLUMNS = QGC_BASE_COLUMNS + ["nomearquivo"]

def ensure_qgc_table(bq: bigquery.Client):
    table_id = f"{PROJECT_ID}.{BQ_DATASET}.{TABLE_NAME_QGC}"
    try:
        table = bq.get_table(table_id)
        # garante que 'nomearquivo' exista
        existing = {f.name for f in table.schema}
        to_add = [c for c in QGC_FINAL_COLUMNS if c not in existing]
        if to_add:
            new_schema = list(table.schema) + [bigquery.SchemaField(c, "STRING") for c in to_add]
            table.schema = new_schema
            bq.update_table(table, ["schema"])
            print(f"[qgc] Schema atualizado com colunas: {to_add}")
        return
    except NotFound:
        print("[qgc] Tabela não existe. Criando...")
        schema = build_string_schema(QGC_FINAL_COLUMNS)
        table = bigquery.Table(table_id, schema=schema)
        bq.create_table(table)
        print("[qgc] Tabela criada.")

def process_qgc_incremental(bq: bigquery.Client, gcs_bucket: str, gcs_object: str):
    final_table = f"{PROJECT_ID}.{BQ_DATASET}.{TABLE_NAME_QGC}"
    base_name   = os.path.basename(gcs_object)
    print(f"[qgc] INCREMENTAL (por arquivo) → {final_table} a partir de {base_name}")

    # baixa
    storage_client = storage.Client()
    blob = storage_client.bucket(gcs_bucket).blob(gcs_object)
    xlsx_path = os.path.join(tempfile.gettempdir(), f"qgc_{int(time.time())}.xlsx")
    blob.download_to_filename(xlsx_path)

    # lê e normaliza headers
    df = pd.read_excel(xlsx_path)
    df.dropna(how="all", inplace=True)
    df = normalize_headers_df(df)

    # garante colunas base; extras são ignoradas
    for col in QGC_BASE_COLUMNS:
        if col not in df.columns:
            df[col] = None

    # seleciona e ordena as colunas base
    df = df[QGC_BASE_COLUMNS]

    # converte tudo para STRING/None
    for c in df.columns:
        df[c] = df[c].map(to_str_or_none)

    # adiciona a coluna de controle por arquivo
    df["nomearquivo"] = base_name

    # staging JSONL
    ts = int(time.time())
    staging_table = f"{PROJECT_ID}.{BQ_DATASET}._qgc_stage_{ts}"
    jsonl_path = os.path.join(tempfile.gettempdir(), f"qgc_{ts}.jsonl")
    df_to_jsonl(df, jsonl_path)

    ensure_qgc_table(bq)

    # carrega staging (WRITE_TRUNCATE)
    staging_schema = build_string_schema(QGC_FINAL_COLUMNS)
    load_cfg = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        create_disposition=bigquery.CreateDisposition.CREATE_IF_NEEDED,
        schema=staging_schema,
        autodetect=False,
    )
    with open(jsonl_path, "rb") as f:
        load_job = bq.load_table_from_file(f, staging_table, job_config=load_cfg)
    load_job.result()
    print(f"[qgc] Staging carregado: {staging_table}")

    # substitui todos os registros daquele arquivo (idempotência por nomearquivo)
    params = [bigquery.ScalarQueryParameter("file", "STRING", base_name)]
    bq.query(
        f"DELETE FROM `{final_table}` WHERE nomearquivo = @file",
        job_config=bigquery.QueryJobConfig(query_parameters=params),
    ).result()
    print(f"[qgc] Registros antigos removidos para nomearquivo={base_name}")

    insert_cols = ", ".join(QGC_FINAL_COLUMNS)
    insert_sql  = f"INSERT INTO `{final_table}` ({insert_cols}) SELECT {insert_cols} FROM `{staging_table}`"
    bq.query(insert_sql).result()
    print("[qgc] Insert concluído.")

    # limpa staging
    try:
        bq.delete_table(staging_table, not_found_ok=True)
        print("[qgc] Staging removido.")
    except Exception as e:
        print(f"[qgc] Falha ao remover staging: {e}")

# ===================== ENTRYPOINT =====================
def entryPoint(data, context):
    bucket = data.get("bucket")
    name   = data.get("name")
    if not bucket or not name:
        print("Evento sem bucket/name. Ignorando.")
        return

    base = os.path.basename(name)
    bq = bigquery.Client()

    # garante dataset e tabela de status no início (para sempre conseguir logar)
    ensure_dataset(bq, BQ_DATASET)
    ensure_status_table(bq)

    # Regra 1: base_geral.xlsx -> FULL LOAD
    if base.lower() == "base_geral.xlsx":
        try:
            process_base_geral(bq, bucket, name)
            log_status(bq, base, "SUCESSO", "implementado com sucesso")
        except Exception as e:
            # ❌ sem raise: não gera retry
            log_status(bq, base, "ERRO", str(e))
        return

    # Regra 2: qualquer outro .xlsx -> INCREMENTAL QGC
    if base.lower().endswith(".xlsx"):
        try:
            process_qgc_incremental(bq, bucket, name)
            log_status(bq, base, "SUCESSO", "implementado com sucesso")
        except Exception as e:
            # ❌ sem raise: não gera retry
            log_status(bq, base, "ERRO", str(e))
        return

    # sem regra
    print(f"Ignorando objeto sem regra: {name}")
