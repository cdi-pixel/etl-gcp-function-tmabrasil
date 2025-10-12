import os
import json
import time
import uuid
import tempfile
from typing import List
from datetime import datetime

import pandas as pd
from google.cloud import storage, bigquery
from google.api_core.exceptions import NotFound
from google.api_core import retry as g_retry

# ===================== CONFIG =====================
TABLE_NAME_BASE_GERAL   = "base_geral"
TABLE_NAME_QGC          = "qgc"
STATUS_TABLE_NAME       = "status_implantacao"

# seu ambiente
BQ_DATASET = "tmabrasil"
PROJECT_ID = "tmabrasil"

# Colunas FIXAS (100% STRING) para base_legal/base_geral
BASE_FIXED_COLUMNS = [
    "empresa","id","sem_arquivos_digitais","com_qgc_feito","tipo_de_sociedade",
    "capital_aberto","setor_de_atuacao","subsetor","estado","cidade","estado2",
    "vara","vara_especializada","pericia_previa","empresa_pericia_previa",
    "advogado","consultoria_assessoria_financeira_reestruturacao",
    "substituicao_do_aj","destituicao_do_aj",
    "_1o_administrador_judicial","_2o_administrador_judicial","_3o_administrador_judicial",
    "tipo","consolidacao_substancial",
    "tamanho_aproximado_da_rj_valor_da_divida_declarado_nos_documentos_iniciais",
    "passivo_apurado_pelo_aj_no_qgc_do_art_7o_2o",
    "modalidade_de_remuneracao_do_aj","remuneracao_aj_do_passivo",
    "remuneracao_aj_valor_total",
    "remuneracao_aj_r_parcelas_mensais_honorarios_provisorios",
    "remuneracao_aj_r_parcelas_mensais_honorarios_definitivos",
    "quantidade_de_assembleias_para_aprovacao",
    "houve_apresentacao_de_plano_pelos_credores","n_processo",
    "link_processo","link_logo","link_ri_outros","termos","informacoes",
    "data_de_manipulacao","segredo_de_justica","processo_fisico_ou_digital",
    "revisao","status_do_processo",
]

# QGC (11 colunas STRING em UPPERCASE)
QGC_COLUMNS = [
    "GRUPO","EMPRESA","FONTE","CLASSE","SUBCLASSE","CREDOR",
    "MOEDA","VALOR","DATA","NOMEARQUIVO","DATAIMPLEMENTACAO"
]
QGC_BASE_CNT = 9  # GRUPO..DATA

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
        bq.create_table(bigquery.Table(table_id, schema=schema))
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

# ===================== RETRY HELPERS =====================
# Retry genérico com backoff exponencial (até 120s)
_default_retry = g_retry.Retry(
    predicate=g_retry.if_exception_type(Exception),
    initial=1.0,
    maximum=10.0,
    multiplier=2.0,
    deadline=120.0
)

def wait_job(job):
    """Espera job (load/query) com retry/backoff."""
    return _default_retry(job.result)()

# ===== FULL LOAD: base_legal/base_geral (ignora header e usa colunas fixas) =====
def process_base_fixed(bq: bigquery.Client, gcs_bucket: str, gcs_object: str):
    table_id = f"{PROJECT_ID}.{BQ_DATASET}.{TABLE_NAME_BASE_GERAL}"
    print(f"[base_fixed] FULL LOAD → {table_id} de gs://{gcs_bucket}/{gcs_object}")

    storage_client = storage.Client()
    uniq = f"{int(time.time()*1000)}_{uuid.uuid4().hex[:8]}"
    xlsx_path = os.path.join(tempfile.gettempdir(), f"base_fixed_{uniq}.xlsx")
    jsonl_path = os.path.join(tempfile.gettempdir(), f"base_fixed_{uniq}.jsonl")

    blob = storage_client.bucket(gcs_bucket).blob(gcs_object)
    blob.download_to_filename(xlsx_path)

    # lê sem header e descarta a primeira linha (header humano)
    df = pd.read_excel(xlsx_path, header=None)
    df.dropna(how="all", inplace=True)
    df.reset_index(drop=True, inplace=True)
    if len(df) >= 1:
        df = df.iloc[1:].reset_index(drop=True)

    # força número e nomes de colunas
    need_cols = len(BASE_FIXED_COLUMNS)
    if df.shape[1] < need_cols:
        for _ in range(need_cols - df.shape[1]):
            df[df.shape[1]] = None
    if df.shape[1] > need_cols:
        df = df.iloc[:, :need_cols]
    df.columns = BASE_FIXED_COLUMNS

    # tudo STRING
    for c in df.columns:
        df[c] = df[c].map(to_str_or_none)

    # gera JSONL e carrega com WRITE_TRUNCATE
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
        load_job = bq.load_table_from_file(f, table_id, job_config=job_config)
    wait_job(load_job)

    print("[base_fixed] FULL LOAD concluído.")

# =========== INCREMENTAL: QGC (aceita 8 ou 9 colunas de dados) ===========
def ensure_qgc_table(bq: bigquery.Client):
    table_id = f"{PROJECT_ID}.{BQ_DATASET}.{TABLE_NAME_QGC}"
    try:
        table = bq.get_table(table_id)
        existing = {f.name for f in table.schema}
        to_add = [c for c in QGC_COLUMNS if c not in existing]
        if to_add:
            new_schema = list(table.schema) + [bigquery.SchemaField(c, "STRING") for c in to_add]
            table.schema = new_schema
            bq.update_table(table, ["schema"])
            print(f"[qgc] Schema atualizado com: {to_add}")
        return
    except NotFound:
        bq.create_table(bigquery.Table(table_id, schema=build_string_schema(QGC_COLUMNS)))
        print("[qgc] Tabela criada.")

def process_qgc_incremental(bq: bigquery.Client, gcs_bucket: str, gcs_object: str):
    final_table = f"{PROJECT_ID}.{BQ_DATASET}.{TABLE_NAME_QGC}"
    base_name   = os.path.basename(gcs_object)
    now_str     = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[qgc] INCREMENTAL → {final_table} de {base_name}")

    storage_client = storage.Client()
    uniq = f"{int(time.time()*1000)}_{uuid.uuid4().hex[:8]}"
    xlsx_path  = os.path.join(tempfile.gettempdir(), f"qgc_{uniq}.xlsx")
    jsonl_path = os.path.join(tempfile.gettempdir(), f"qgc_{uniq}.jsonl")

    blob = storage_client.bucket(gcs_bucket).blob(gcs_object)
    blob.download_to_filename(xlsx_path)

    # Lê SEM header (aceita 8 ou 9 colunas)
    df = pd.read_excel(xlsx_path, header=None)

    # Remove linhas totalmente vazias e descarta a 1ª (header humano)
    df.dropna(how="all", inplace=True)
    df.reset_index(drop=True, inplace=True)
    if len(df) >= 1:
        df = df.iloc[1:].reset_index(drop=True)

    # Remove colunas 100% vazias (corrige colunas “fantasmas”)
    if df.shape[1] > 0:
        df = df.dropna(axis=1, how="all").reset_index(drop=True)

    # Tratamento SUBCLASSE ausente:
    # Se vierem 8 colunas (sem SUBCLASSE), insere coluna vazia na posição 4 (entre CLASSE e CREDOR).
    if df.shape[1] == 8:
        df.insert(4, "__SUBCLASSE_MISSING__", None)

    # Garante exatamente 9 colunas de dados (GRUPO..DATA)
    if df.shape[1] < QGC_BASE_CNT:
        for _ in range(QGC_BASE_CNT - df.shape[1]):
            df[df.shape[1]] = None
    if df.shape[1] > QGC_BASE_CNT:
        df = df.iloc[:, :QGC_BASE_CNT]

    # Nomeia as 9 colunas base
    df.columns = QGC_COLUMNS[:QGC_BASE_CNT]

    # Colunas de controle
    df["NOMEARQUIVO"]       = base_name
    df["DATAIMPLEMENTACAO"] = now_str

    # Reordena para as 11 finais
    for col in QGC_COLUMNS:
        if col not in df.columns:
            df[col] = None
    df = df[QGC_COLUMNS]

    # STRING
    for c in df.columns:
        df[c] = df[c].map(to_str_or_none)

    # Staging -> Load -> (TX) DELETE por NOMEARQUIVO -> INSERT
    ensure_qgc_table(bq)
    staging_table = f"{PROJECT_ID}.{BQ_DATASET}._qgc_stage_{uniq}"

    df_to_jsonl(df, jsonl_path)
    load_cfg = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        create_disposition=bigquery.CreateDisposition.CREATE_IF_NEEDED,
        schema=build_string_schema(QGC_COLUMNS),
        autodetect=False,
    )

    with open(jsonl_path, "rb") as f:
        load_job = bq.load_table_from_file(f, staging_table, job_config=load_cfg)
    wait_job(load_job)

    # Define expiração curta no staging pra evitar lixo
    try:
        table_obj = bq.get_table(staging_table)
        # 30 minutos
        table_obj.expires = datetime.utcnow() + pd.Timedelta(minutes=30)
        bq.update_table(table_obj, ["expires"])
    except Exception as e:
        print(f"[qgc] Aviso: não defini expiração do staging: {e}")

    # Transação: DELETE + INSERT (minimiza janela de corrida)
    qcfg = bigquery.QueryJobConfig(
        query_parameters=[bigquery.ScalarQueryParameter("file", "STRING", base_name)]
    )
    sql = f"""
    BEGIN TRANSACTION;
      DELETE FROM `{final_table}` WHERE NOMEARQUIVO = @file;
      INSERT INTO `{final_table}` ({', '.join(QGC_COLUMNS)})
      SELECT {', '.join(QGC_COLUMNS)} FROM `{staging_table}`;
    COMMIT TRANSACTION;
    """
    query_job = bq.query(sql, job_config=qcfg)
    wait_job(query_job)

    # Limpeza do staging
    try:
        bq.delete_table(staging_table, not_found_ok=True)
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
    bq = bigquery.Client(project=PROJECT_ID)

    ensure_dataset(bq, BQ_DATASET)
    ensure_status_table(bq)

    # base_legal/base_geral -> FULL LOAD com colunas fixas (ignora header humano)
    if base.lower() in ("base_legal.xlsx", "base_geral.xlsx"):
        try:
            process_base_fixed(bq, bucket, name)
            log_status(bq, base, "SUCESSO", "implementado com sucesso")
        except Exception as e:
            log_status(bq, base, "ERRO", str(e))
        return

    # demais .xlsx -> QGC incremental (11 colunas, SUBCLASSE opcional)
    if base.lower().endswith(".xlsx"):
        try:
            process_qgc_incremental(bq, bucket, name)
            log_status(bq, base, "SUCESSO", "implementado com sucesso")
        except Exception as e:
            log_status(bq, base, "ERRO", str(e))
        return

    print(f"Ignorando objeto sem regra: {name}")
