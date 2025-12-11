import os
import json
import time
import uuid
import tempfile
from datetime import datetime
from typing import List, Dict
import pandas as pd
from google.cloud import storage, bigquery
from google.api_core.exceptions import NotFound, BadRequest, GoogleAPICallError

# ===================== CONFIG =====================
TABLE_NAME_BASE_GERAL   = "base_geral"
TABLE_NAME_QGC          = "qgc"
STATUS_TABLE_NAME       = "status_implantacao"

BQ_DATASET = "tmabrasil"
PROJECT_ID = "tmabrasil"

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

QGC_COLUMNS = [
    "GRUPO","EMPRESA","FONTE","CLASSE","SUBCLASSE","CREDOR",
    "MOEDA","VALOR","DATA",
    
    "DATA_PRIMEIRA_AGC","PTAX","FONTE_COTACAO","VALOR_BRL", 
    "NOMEARQUIVO","DATAIMPLEMENTACAO"
]

EXPECTED_WITH_SUB = ["GRUPO","EMPRESA","FONTE","CLASSE","SUBCLASSE","CREDOR","MOEDA","VALOR","DATA","DATA_PRIMEIRA_AGC","PTAX","FONTE_COTACAO","VALOR_BRL"]
EXPECTED_NO_SUB   = ["GRUPO","EMPRESA","FONTE","CLASSE","CREDOR","MOEDA","VALOR","DATA","DATA_PRIMEIRA_AGC","PTAX","FONTE_COTACAO","VALOR_BRL"]

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

def _slug(s: str):
    import re, unicodedata
    s = unicodedata.normalize("NFKD", str(s)).encode("ascii", "ignore").decode("ascii")
    s = s.lower()
    s = re.sub(r"[^a-z0-9_]+", "_", s)
    return s.strip("_")

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

def ensure_status_table(bq: bigquery.Client):
    table_id = f"{PROJECT_ID}.{BQ_DATASET}.{STATUS_TABLE_NAME}"
    try:
        bq.get_table(table_id)
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
            "status": (status or "")[:50],
            "mensagem": (mensagem or "")[:1500],
        }]
        bq.insert_rows_json(table_id, rows)
        print(f"[status] {status} | {nome_arquivo} | {mensagem}")
    except Exception as e:
        print(f"[status] Falha ao registrar status: {e}")

# ===================== BASE GERAL =====================
def _pick_sheet(xlsx_path: str) -> str:
    xf = pd.ExcelFile(xlsx_path)
    wanted = {"lista_de_informacoes", "lista_de_informacao"}
    slug_map = {sh: _slug(sh) for sh in xf.sheet_names}
    for sh, sl in slug_map.items():
        if sl in wanted:
            print(f"[base_fixed] Aba selecionada: '{sh}' (slug='{sl}')")
            return sh
    print(f"[base_fixed] Aba 'lista de informações' NÃO encontrada. Usando primeira: {xf.sheet_names[0]}")
    return xf.sheet_names[0]

def process_base_fixed(bq: bigquery.Client, gcs_bucket: str, gcs_object: str):
    table_id = f"{PROJECT_ID}.{BQ_DATASET}.{TABLE_NAME_BASE_GERAL}"
    print(f"[base_fixed] FULL LOAD → {table_id} de gs://{gcs_bucket}/{gcs_object}")

    storage_client = storage.Client()
    uniq = f"{int(time.time()*1000)}_{uuid.uuid4().hex[:8]}"
    xlsx_path = os.path.join(tempfile.gettempdir(), f"base_fixed_{uniq}.xlsx")
    jsonl_path = os.path.join(tempfile.gettempdir(), f"base_fixed_{uniq}.jsonl")
    storage_client.bucket(gcs_bucket).blob(gcs_object).download_to_filename(xlsx_path)

    sheet = _pick_sheet(xlsx_path)
    df = pd.read_excel(xlsx_path, header=0, sheet_name=sheet)
    df.dropna(how="all", inplace=True)
    df.reset_index(drop=True, inplace=True)

    slugged = [_slug(c) for c in df.columns]
    mapping = dict(zip(slugged, df.columns))

    std = pd.DataFrame(columns=BASE_FIXED_COLUMNS)
    for col in BASE_FIXED_COLUMNS:
        slug = _slug(col)
        src = mapping.get(slug)
        std[col] = df[src] if (src in df.columns) else None

    for c in std.columns:
        std[c] = std[c].map(to_str_or_none)

    df_to_jsonl(std, jsonl_path)
    schema = build_string_schema(BASE_FIXED_COLUMNS)
    cfg = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        schema=schema
    )
    with open(jsonl_path, "rb") as f:
        bq.load_table_from_file(f, table_id, job_config=cfg).result()
    print("[base_fixed] FULL LOAD concluído.")

# ===================== QGC: VALIDAÇÃO DE HEADER =====================
def _norm(s):
    return str(s or "").strip().upper()

def validate_qgc_header(header_row):
    """
    header_row: list-like com a primeira linha da planilha (nomes das colunas)
    Retorna (ok: bool, has_subclasse: bool, error_msg: str|None)
    Regras:
      - Aceita exatamente EXPECTED_WITH_SUB (13 colunas) OU EXPECTED_NO_SUB (12 colunas).
      - Colunas obrigatórias (sem SUBCLASSE): GRUPO,EMPRESA,FONTE,CLASSE,CREDOR,MOEDA,VALOR,DATA,DATA_PRIMEIRA_AGC,PTAX,FONTE_COTACAO,VALOR_BRL
      - Ordem deve bater 100%.
    """
    if header_row is None:
        return (False, False, "Cabeçalho não encontrado")

    header = [_norm(x) for x in list(header_row)]
    # Remove colunas vazias no fim
    while header and header[-1] == "":
        header.pop()

    required = ["GRUPO","EMPRESA","FONTE","CLASSE","CREDOR","MOEDA","VALOR","DATA","DATA_PRIMEIRA_AGC","PTAX","FONTE_COTACAO","VALOR_BRL"]
    for r in required:
        if r not in header:
            return (False, False, f"{r} não veio")

    # Checar ordem com SUBCLASSE (13 primeiras)
    if len(header) >= 13:
        first9 = header[:13]
        if all(first9[i] == EXPECTED_WITH_SUB[i] for i in range(13)):
            return (True, True, None)

    # Checar ordem sem SUBCLASSE (12 primeiras)
    if len(header) >= 12:
        first8 = header[:12]
        if all(first8[i] == EXPECTED_NO_SUB[i] for i in range(12)):
            return (True, False, None)

    got = ", ".join(header[:13])
    exp1 = ", ".join(EXPECTED_WITH_SUB)
    exp2 = ", ".join(EXPECTED_NO_SUB)
    return (False, False, f"Ordem inválida. Esperado: [{exp1}] ou [{exp2}] — Recebido: [{got}]")

# ===================== QGC INCREMENTAL =====================
def ensure_qgc_table(bq: bigquery.Client):
    table_id = f"{PROJECT_ID}.{BQ_DATASET}.{TABLE_NAME_QGC}"
    try:
        bq.get_table(table_id)
    except NotFound:
        bq.create_table(bigquery.Table(table_id, schema=build_string_schema(QGC_COLUMNS)))
        print("[qgc] Tabela criada.")
    else:
        print("[qgc] Tabela existente.")

def process_qgc_incremental(bq: bigquery.Client, gcs_bucket: str, gcs_object: str):
    final_table = f"{PROJECT_ID}.{BQ_DATASET}.{TABLE_NAME_QGC}"
    base_name   = os.path.basename(gcs_object)
    now_str     = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[qgc] INCREMENTAL → {final_table} de {base_name}")

    storage_client = storage.Client()
    uniq = f"{int(time.time()*1000)}_{uuid.uuid4().hex[:8]}"
    xlsx_path = os.path.join(tempfile.gettempdir(), f"qgc_{uniq}.xlsx")
    blob = storage_client.bucket(gcs_bucket).blob(gcs_object)
    blob.download_to_filename(xlsx_path)

    # Ler tudo como matriz (preservando header visual)
    df_matrix = pd.read_excel(xlsx_path, header=None)
    # Remover linhas totalmente vazias
    df_matrix = df_matrix.dropna(how="all")
    if df_matrix.empty:
        raise BadRequest("Planilha vazia.")

    # HEADER BRUTO (primeira linha)
    header_row = df_matrix.iloc[0, :].tolist()
    ok, has_subclasse, err = validate_qgc_header(header_row)
    if not ok:
        # Sinalizar erro ao usuário via status e abortar import
        raise BadRequest(err)

    # Dados (sem o header)
    data_only = df_matrix.iloc[1:, :].copy()
    # Normaliza cada linha para 9 colunas (GRUPO..DATA), inserindo SUBCLASSE vazia quando necessário
    normalized_rows = []
    for _, row in data_only.iterrows():
        arr = row.tolist()
        # Limpa trailing vazios (caso tenha colunas a mais em branco no fim)
        while len(arr) and (arr[-1] is None or str(arr[-1]).strip() == ""):
            arr.pop()

        # Se não tem SUBCLASSE (12 colunas), inserir None na posição 4
        if not has_subclasse and len(arr) >= 12:
            arr = arr[:4] + [None] + arr[4:]

        # Completar até 13 (GRUPO..DATA); cortar se > 13
        if len(arr) < 13:
            arr.extend([None] * (13 - len(arr)))
        elif len(arr) > 13:
            arr = arr[:13]

        normalized_rows.append(arr)

    # Monta DataFrame com 13 primeiras colunas ordenadas e adiciona NOMEARQUIVO/DATAIMPLEMENTACAO
    df = pd.DataFrame(normalized_rows, columns=QGC_COLUMNS[:13])
    df["NOMEARQUIVO"] = base_name
    df["DATAIMPLEMENTACAO"] = now_str

    for c in df.columns:
        df[c] = df[c].map(to_str_or_none)

    staging = f"{PROJECT_ID}.{BQ_DATASET}._stage_{uniq}"
    jsonl = os.path.join(tempfile.gettempdir(), f"stage_{uniq}.jsonl")
    df_to_jsonl(df[QGC_COLUMNS], jsonl)
    ensure_qgc_table(bq)

    load_cfg = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        write_disposition="WRITE_TRUNCATE",
        schema=build_string_schema(QGC_COLUMNS)
    )
    with open(jsonl, "rb") as f:
        bq.load_table_from_file(f, staging, job_config=load_cfg).result()

    sql = f"""
    MERGE `{final_table}` AS T
    USING `{staging}` AS S
    ON T.NOMEARQUIVO = S.NOMEARQUIVO
    WHEN MATCHED THEN DELETE
    WHEN NOT MATCHED BY TARGET THEN
      INSERT ({', '.join(QGC_COLUMNS)})
      VALUES ({', '.join('S.' + c for c in QGC_COLUMNS)});
    """

    for attempt in range(1, 12):
        try:
            bq.query(sql).result()
            print(f"[qgc] MERGE concluído ({attempt} tentativa).")
            break
        except (BadRequest, GoogleAPICallError) as e:
            msg = str(e)
            if "Transaction is aborted" in msg or "concurrent update" in msg:
                delay = min(2 ** attempt, 10)
                print(f"[qgc] Conflito de concorrência, retry em {delay}s (tentativa {attempt})")
                time.sleep(delay)
                continue
            raise
    bq.delete_table(staging, not_found_ok=True)
    print("[qgc] Incremental concluído.")

# ===================== ENTRYPOINT =====================
def entryPoint(data, context):
    bucket = data.get("bucket")
    name   = data.get("name")
    if not bucket or not name:
        print("Evento sem bucket/name. Ignorando.")
        return

    base = os.path.basename(name)
    bq_client = bigquery.Client()
    ensure_dataset(bq_client, BQ_DATASET)
    ensure_status_table(bq_client)

    try:
        if base.lower() in ("base_geral.xlsx", "base_legal.xlsx"):
            process_base_fixed(bq_client, bucket, name)
        elif base.lower().endswith(".xlsx"):
            process_qgc_incremental(bq_client, bucket, name)
        log_status(bq_client, base, "SUCESSO", "Implementado com sucesso.")
    except Exception as e:
        # Registro de erro e propagação p/ a plataforma entender que falhou
        log_status(bq_client, base, "ERRO", str(e))
        raise
