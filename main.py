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
STATUS_TABLE_NAME       = "status_implantacao"

# seu ambiente
BQ_DATASET = "tmabrasil"
PROJECT_ID = "tmabrasil"

# Colunas FIXAS (100% STRING) para base_legal/base_geral
BASE_FIXED_COLUMNS = [
    "empresa",
    "id",
    "sem_arquivos_digitais",
    "com_qgc_feito",
    "tipo_de_sociedade",
    "capital_aberto",
    "setor_de_atuacao",
    "subsetor",
    "estado",
    "cidade",
    "estado2",
    "vara",
    "vara_especializada",
    "pericia_previa",
    "empresa_pericia_previa",
    "advogado",
    "consultoria_assessoria_financeira_reestruturacao",
    "substituicao_do_aj",
    "destituicao_do_aj",
    "_1o_administrador_judicial",
    "_2o_administrador_judicial",
    "_3o_administrador_judicial",
    "tipo",
    "consolidacao_substancial",
    "tamanho_aproximado_da_rj_valor_da_divida_declarado_nos_documentos_iniciais",
    "passivo_apurado_pelo_aj_no_qgc_do_art_7o_2o",
    "modalidade_de_remuneracao_do_aj",
    "remuneracao_aj_do_passivo",
    "remuneracao_aj_valor_total",
    "remuneracao_aj_r_parcelas_mensais_honorarios_provisorios",
    "remuneracao_aj_r_parcelas_mensais_honorarios_definitivos",
    "quantidade_de_assembleias_para_aprovacao",
    "houve_apresentacao_de_plano_pelos_credores",
    "n_processo",
    "link_processo",
    "link_logo",
    "link_ri_outros",
    "termos",
    "informacoes",
    "data_de_manipulacao",
    "segredo_de_justica",
    "processo_fisico_ou_digital",
    "revisao",
    "status_do_processo",
]

# ===================== HELPERS =====================
def to_str_or_none(v):
    if v is None:
        return None
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

# ====== FULL LOAD: base_legal/base_geral (ignora header e usa colunas fixas) ======
def process_base_fixed(bq: bigquery.Client, gcs_bucket: str, gcs_object: str):
    table_id = f"{PROJECT_ID}.{BQ_DATASET}.{TABLE_NAME_BASE_GERAL}"
    print(f"[base_fixed] FULL LOAD (colunas fixas) → {table_id} a partir de gs://{gcs_bucket}/{gcs_object}")

    storage_client = storage.Client()
    blob = storage_client.bucket(gcs_bucket).blob(gcs_object)
    xlsx_path = os.path.join(tempfile.gettempdir(), "base_fixed.xlsx")
    blob.download_to_filename(xlsx_path)

    # 1) lê SEM header (header=None)
    df = pd.read_excel(xlsx_path, header=None)
    # 2) remove linhas totalmente vazias
    df.dropna(how="all", inplace=True)
    df.reset_index(drop=True, inplace=True)
    # 3) descarta a primeira linha (header humano)
    if len(df) >= 1:
        df = df.iloc[1:].reset_index(drop=True)

    # 4) força o número de colunas para o tamanho fixo
    need_cols = len(BASE_FIXED_COLUMNS)
    # se tem menos colunas, completa com None
    if df.shape[1] < need_cols:
        for _ in range(need_cols - df.shape[1]):
            df[df.shape[1]] = None
    # se tem mais colunas, corta
    if df.shape[1] > need_cols:
        df = df.iloc[:, :need_cols]
    # aplica os nomes fixos
    df.columns = BASE_FIXED_COLUMNS

    # 5) tudo STRING/None
    for c in df.columns:
        df[c] = df[c].map(to_str_or_none)

    # 6) JSONL + LOAD (WRITE_TRUNCATE)
    jsonl_path = os.path.join(tempfile.gettempdir(), "base_fixed.jsonl")
    df_to_jsonl(df, jsonl_path)

    schema = build_string_schema(BASE_FIXED_COLUMNS)
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
    print(f"[base_fixed] FULL LOAD concluído: {table.num_rows} linhas")

# =========== INCREMENTAL: QGC (nomearquivo + data_inclusao, substitui por arquivo) ===========
QGC_BASE_COLUMNS  = ["grupo","empresa","fonte","classe","subclasse","credor","moeda","valor","data"]
QGC_FINAL_COLUMNS = QGC_BASE_COLUMNS + ["nomearquivo", "data_inclusao"]

def ensure_qgc_table(bq: bigquery.Client):
    table_id = f"{PROJECT_ID}.{BQ_DATASET}.{TABLE_NAME_QGC}"
    try:
        table = bq.get_table(table_id)
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
    now_str     = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[qgc] INCREMENTAL (por arquivo) → {final_table} a partir de {base_name}")

    storage_client = storage.Client()
    blob = storage_client.bucket(gcs_bucket).blob(gcs_object)
    xlsx_path = os.path.join(tempfile.gettempdir(), f"qgc_{int(time.time())}.xlsx")
    blob.download_to_filename(xlsx_path)

    # aqui mantemos leitura comum com header na 1ª linha (se preferir, dá pra fazer como no base_fixed)
    df = pd.read_excel(xlsx_path)
    df.dropna(how="all", inplace=True)

    # garante colunas base (cria faltantes), depois ordena e corta/extrai só elas
    for col in QGC_BASE_COLUMNS:
        if col not in df.columns:
            df[col] = None
    df = df[QGC_BASE_COLUMNS]

    for c in df.columns:
        df[c] = df[c].map(to_str_or_none)

    df["nomearquivo"]   = base_name
    df["data_inclusao"] = now_str

    # staging
    ts = int(time.time())
    staging_table = f"{PROJECT_ID}.{BQ_DATASET}._qgc_stage_{ts}"
    jsonl_path = os.path.join(tempfile.gettempdir(), f"qgc_{ts}.jsonl")
    df_to_jsonl(df, jsonl_path)

    ensure_qgc_table(bq)

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

    # substitui todo o conteúdo do mesmo arquivo
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

    ensure_dataset(bq, BQ_DATASET)
    ensure_status_table(bq)

    # base_legal.xlsx OU base_geral.xlsx -> FULL LOAD com colunas fixas, ignorando header
    if base.lower() in ("base_legal.xlsx", "base_geral.xlsx"):
        try:
            process_base_fixed(bq, bucket, name)
            log_status(bq, base, "SUCESSO", "implementado com sucesso")
        except Exception as e:
            log_status(bq, base, "ERRO", str(e))
        return

    # qualquer outro .xlsx -> QGC incremental
    if base.lower().endswith(".xlsx"):
        try:
            process_qgc_incremental(bq, bucket, name)
            log_status(bq, base, "SUCESSO", "implementado com sucesso")
        except Exception as e:
            log_status(bq, base, "ERRO", str(e))
        return

    print(f"Ignorando objeto sem regra: {name}")
