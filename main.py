# main.py — assinatura background (data, context)
# Não precisa de Flask nem de cloudevents.

def entryPoint(data, context):
    # data é um dict com info do objeto no GCS
    bucket = data.get("bucket")
    name   = data.get("name")        # caminho dentro do bucket (ex.: "minha-pasta/arquivo.xlsx")
    size   = data.get("size")

    # Filtra só .xlsx (e opcionalmente uma pasta prefixo)
    if not name or not name.endswith(".xlsx"):
        print(f"Ignorando objeto: {name}")
        return

    # Se quiser exigir uma pasta específica:
    # if not name.startswith("minha-pasta/"):
    #     print(f"Ignorando fora da pasta: {name}")
    #     return

    print(f"Novo XLSX: gs://{bucket}/{name} (size={size})")
    # TODO: seu processamento aqui...
    # Ex.: baixar o arquivo, parsear, etc.
    # Dica: para ler direto do GCS use google-cloud-storage (adicione no requirements.txt)

    return  # sem resposta HTTP; é função de evento
