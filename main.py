import os
import json
import time
import uuid
import tempfile
import unicodedata
from typing import List, Dict, Tuple
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

# Colunas FIXAS (100% STRING) para base_legal/base_geral (target no BQ)
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

def _slug(s: str) -> str:
    """
    Normaliza cabeçalhos:
    - remove acentos (NFKD)
    - 'º'/'°' -> 'o'
    - '§' vira 's' (e números ficam), '%' -> 'pct'
    - 'R$' -> 'rs'
    - '/' '-' e espaços -> '_'
    - remove/normaliza pontuação, mantém [a-z0-9_]
    """
    if s is None:
        return ""
    s = str(s).strip()
    s = s.replace("R$", "rs")
    s = s.replace("º", "o").replace("°", "o")
    s = s.replace("%", "pct")
    s = s.replace("/", "_").replace("-", " ")
    # trata § (mantém números ao redor, ex: "§2º" -> "s2o")
    s = s.replace("§", "s")
    s = unicodedata.normalize("NFKD", s)
    s = "".join(ch for ch in s if not unicodedata.combining(ch))
    out = []
    for ch in s:
        if ch.isalnum():
            out.append(ch.lower())
        elif ch in [" ", "_"]:
            out.append("_")
        else:
            out.append("_")
    s = "".join(out)
    s = "_".join(filter(None, s.split("_")))
    return s

# mapa de sinônimos de headers → coluna-alvo (usando _slug)
HEADER_ALIASES: Dict[str, str] = {
    # ==== mapeamento exato do header informado ====
    "empresa": "empresa",
    "id": "id",
    "sem_arquivos_digitais": "sem_arquivos_digitais",
    "com_qgc_feito": "com_qgc_feito",
    "capital_aberto": "capital_aberto",
    "setor_de_atuacao": "setor_de_atuacao",
    "subsetor": "subsetor",
    "estado": "estado",
    "cidade": "cidade",
    "estado2": "estado2",
    "vara": "vara",
    "vara_especializada": "vara_especializada",  # "VARA ESPECIALIZADA?"
    "pericia_previa": "pericia_previa",          # "PERÍCIA PRÉVIA?"
    "empresa_pericia_previa": "empresa_pericia_previa",
    "advogado": "advogado",
    "consultoria_assessoria_financeira_reestruturacao": "consultoria_assessoria_financeira_reestruturacao",
    "substituicao_do_aj": "substituicao_do_aj",
    "destituicao_do_aj": "destituicao_do_aj",
    "1o_administrador_judicial": "_1o_administrador_judicial",
    "2o_administrador_judicial": "_2o_administrador_judicial",
    "3o_administrador_judicial": "_3o_administrador_judicial",
    "tipo": "tipo",
    "tamanho_aproximado_da_rj_valor_da_divida_declarado_nos_documentos_iniciais":
        "tamanho_aproximado_da_rj_valor_da_divida_declarado_nos_documentos_iniciais",
    "passivo_apurado_pelo_aj_no_qgc_do_art_7o_2o": "passivo_apurado_pelo_aj_no_qgc_do_art_7o_2o",  # "§2º" -> "7o_2o"
    "modalidade_de_remuneracao_do_aj": "modalidade_de_remuneracao_do_aj",
    "remuneracao_aj_pct_do_passivo": "remuneracao_aj_do_passivo",  # "REMUNERAÇÃO AJ (% DO PASSIVO)"
    "remuneracao_aj_do_passivo": "remuneracao_aj_do_passivo",
    "remuneracao_aj_valor_total": "remuneracao_aj_valor_total",    # "REMUNERAÇÃO AJ - VALOR TOTAL"
    "remuneracao_aj_rs_parcelas_mensais_honorarios_provisorios":
        "remuneracao_aj_r_parcelas_mensais_honorarios_provisorios",
    "remuneracao_aj_rs_parcelas_mensais_honorarios_definitivos":
        "remuneracao_aj_r_parcelas_mensais_honorarios_definitivos",
    "quantidade_de_assembleias_para_aprovacao": "quantidade_de_assembleias_para_aprovacao",
    "houve_apresentacao_de_plano_pelos_credores": "houve_apresentacao_de_plano_pelos_credores",
    "n_processo": "n_processo",                 # cobre "N PROCESSO"
    "link_processo": "link_processo",
    "link_logo": "link_logo",
    "link_ri_outros": "link_ri_outros",
    "termos": "termos",
    "informacoes": "informacoes",
    "data_de_manipulacao": "data_de_manipulacao",
    "segredo_de_justica": "segredo_de_justica",
    "processo_fisico_ou_digital": "processo_fisico_ou_digital",
    "revisao": "revisao",                       # "REVISÃO"
    "status_processo": "status_do_processo",    # "STATUS PROCESSO"
    # ==== sinônimos usuais ====
    "uf": "estado",
    "numero_do_processo": "n_processo",
    "_1o_administrador_judicial": "_1o_administrador_judicial",
    "_2o_administrador_judicial": "_2o_administrador_judicial",
    "_3o_administrador_judicial": "_3o_administrador_judicial",
}

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
_default_retry = g_retry.Retry(
    predicate=g_retry.if_exception_type(Exception),
    initial=1.0,
    maximum=10.0,
    multiplier=2.0,
    deadline=120.0
)
def wait_job(job):
    return _default_retry(job.result)()

# ===== Header mapping helpers =====
def _map_headers_to_targets(cols: List[str]) -> Dict[int, str]:
    """Retorna um mapa idx->col_alvo quando der para casar por nome."""
    slugs = [_slug(c) for c in cols]
    mapping: Dict[int, str] = {}
    for i, s in enumerate(slugs):
        if s in HEADER_ALIASES:
            mapping[i] = HEADER_ALIASES[s]
        elif s in BASE_FIXED_COLUMNS:
            mapping[i] = s
        else:
            # heurísticas simples para os "1º/2º/3º administrador"
            if s.startswith("1") and "administrador" in s:
                mapping[i] = "_1o_administrador_judicial"
            elif s.startswith("2") and "administrador" in s:
                mapping[i] = "_2o_administrador_judicial"
            elif s.startswith("3") and "administrador" in s:
                mapping[i] = "_3o_administrador_judicial"
    return mapping

def _looks_like_header(row0: List[str]) -> bool:
    """Se maioria das células são strings com letras, assume que é header."""
    texts = 0
    for v in row0:
        if isinstance(v, str) and any(ch.isalpha() for ch in v):
            texts += 1
    return texts >= max(3, int(len(row0) * 0.4))

# ===== FULL LOAD: base_legal/base_geral =====
def process_base_fixed(bq: bigquery.Client, gcs_bucket: str, gcs_object: str):
    table_id = f"{PROJECT_ID}.{BQ_DATASET}.{TABLE_NAME_BASE_GERAL}"
    print(f"[base_fixed] FULL LOAD → {table_id} de gs://{gcs_bucket}/{gcs_object}")

    storage_client = storage.Client()
    uniq = f"{int(time.time()*1000)}_{uuid.uuid4().hex[:8]}"
    xlsx_path = os.path.join(tempfile.gettempdir(), f"base_fixed_{uniq}.xlsx")
    jsonl_path = os.path.join(tempfile.gettempdir(), f"base_fixed_{uniq}.jsonl")
    blob = storage_client.bucket(gcs_bucket).blob(gcs_object)
    blob.download_to_filename(xlsx_path)

    # Tenta ler com header na primeira linha
    df_try = pd.read_excel(xlsx_path, header=0)
    use_header = _looks_like_header(list(df_try.columns))

    if use_header:
        header_mapping = _map_headers_to_targets(list(df_try.columns))
        df = df_try.copy()
        df.dropna(how="all", inplace=True)
        df.reset_index(drop=True, inplace=True)
    else:
        # Sem header “crível”: lê sem header e NÃO descarta a primeira linha
        df = pd.read_excel(xlsx_path, header=None)
        df.dropna(how="all", inplace=True)
        df.reset_index(drop=True, inplace=True)
        # fallback posicional: casa 1:1 até o tamanho de BASE_FIXED_COLUMNS
        header_mapping = {i: BASE_FIXED_COLUMNS[i] for i in range(min(df.shape[1], len(BASE_FIXED_COLUMNS)))}

    # DataFrame alvo com todas as colunas, inicializadas como None
    std = pd.DataFrame(index=range(len(df)))
    for c in BASE_FIXED_COLUMNS:
        std[c] = None

    # Preenche o que casou por nome/posição
    for src_idx, tgt_col in header_mapping.items():
        if src_idx < df.shape[1] and tgt_col in std.columns:
            std.loc[:, tgt_col] = df.iloc[:, src_idx]

    # Tudo string
    for c in std.columns:
        std[c] = std[c].map(to_str_or_none)

    # Gera JSONL e carrega com WRITE_TRUNCATE
    df_to_jsonl(std, jsonl_path)
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
    wait_job(job)

    print("[base_fixed] FULL LOAD concluído.")

# =========== INCREMENTAL: QGC ===========
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
    df.dropna(how="all", inplace=True)
    df.reset_index(drop=True, inplace=True)
    if len(df) >= 1:
        # descarta a 1ª (header humano) para QGC
        df = df.iloc[1:].reset_index(drop=True)

    if df.shape[1] > 0:
        df = df.dropna(axis=1, how="all").reset_index(drop=True)

    # Tratamento SUBCLASSE ausente → 8 colunas
    if df.shape[1] == 8:
        df.insert(4, "__SUBCLASSE_MISSING__", None)

    # Garante exatamente 9 colunas de dados (GRUPO..DATA)
    if df.shape[1] < QGC_BASE_CNT:
        for _ in range(QGC_BASE_CNT - df.shape[1]):
            df[df.shape[1]] = None
    if df.shape[1] > QGC_BASE_CNT:
        df = df.iloc[:, :QGC_BASE_CNT]

    df.columns = QGC_COLUMNS[:QGC_BASE_CNT]
    df["NOMEARQUIVO"]       = base_name
    df["DATAIMPLEMENTACAO"] = now_str

    for col in QGC_COLUMNS:
        if col not in df.columns:
            df[col] = None
    df = df[QGC_COLUMNS]

    for c in df.columns:
        df[c] = df[c].map(to_str_or_none)

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

    # Expiração do staging (30 min)
    try:
        table_obj = bq.get_table(staging_table)
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

    # base_legal/base_geral -> FULL LOAD com mapeamento por header (ou posição se não houver header)
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
