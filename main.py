# CF v2 (Eventarc) usa CloudEvent
# pip: cloudevents
from cloudevents.http import CloudEvent

def entryPoint(event: CloudEvent):
    data = event.data or {}
    bucket = data.get("bucket")
    name = data.get("name")
    size = data.get("size")

    # filtre aqui, se quiser só .xlsx e pasta
    if not name or not name.endswith(".xlsx") or not name.startswith("minha-pasta/"):
        print(f"Ignorando objeto: {name}")
        return

    print(f"Novo XLSX: gs://{bucket}/{name} (size={size})")
    # ... seu processamento ...
