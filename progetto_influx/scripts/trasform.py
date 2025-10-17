import json
from datetime import datetime, timezone

# crea timestamp in ns (UTC)
def parse_ts_ns(ts_str: str) -> int:
    """
    Converte '2025-10-03T07:05:33+00:00' in epoch nanosecondi (int).
    """
    dt = datetime.fromisoformat(ts_str)
    return int(round(dt.timestamp() * 1_000_000_000))
    

# Pulisce le stringhe quando si passa al line protocol, modificando i caratteri speciali ('=' ',' 'whiteline')->table,<key_tags=val_tag> <key_fields=val_field> <tiemstamp>
def escape_measurement(m: str) -> str:
    return m.replace(",", r"\,").replace(" ", r"\ ")

def escape_tag_key(k: str) -> str:
    return k.replace(",", r"\,").replace(" ", r"\ ").replace("=", r"\=")

def escape_tag_val(v: str) -> str:
    return str(v).replace(",", r"\,").replace(" ", r"\ ").replace("=", r"\=")

def escape_field_key(k: str) -> str:
    return k.replace(",", r"\,").replace(" ", r"\ ").replace("=", r"\=")
# imposta i valori dei fields conformi a lp
def format_field_val(v):
    """
    Regole Influx:
    - int -> '123i'
    - float -> '123.45'
    - bool -> 'true'/'false'
    - str -> "text" con doppi apici
    """
    if isinstance(v, bool):
        return "true" if v else "false"
    if isinstance(v, int):
        return f"{v}i"
    if isinstance(v, float):
        return repr(v)
    # Stringa: escape dei doppi apici
    s = str(v).replace('"', r'\"')
    return f"\"{s}\""

#Costruisce LP, se i campi non sono presenti ({}) ritorna "" (influx richiede field)
def build_line(measurement: str, tags: dict, fields: dict, ts_ns: int) -> str:
    if not fields:
        return ""
    m = escape_measurement(measurement)
    tag_part = ",".join(f"{escape_tag_key(k)}={escape_tag_val(v)}"
                        for k, v in sorted(tags.items()))
    field_part = ",".join(f"{escape_field_key(k)}={format_field_val(v)}"
                          for k, v in sorted(fields.items()))
    if tag_part:
        return f"{m},{tag_part} {field_part} {ts_ns}"
    else:
        return f"{m} {field_part} {ts_ns}"
        
# Nel messaggio quotas è una chiave appartenente al dizionario della measurement che stiamo considerando, ha come valore una lista di due elementi: risorse realmente usate e risorse a disposizione ("usage"=true/false). Questa funzione serve a verificare la presenza di quotas, verificare che sia una lista e selezionare le risorse disponibili o usate.
def find_quota(quotas: list, usage_flag: bool):
    if not isinstance(quotas, list):
        return None
    for q in quotas:
        if isinstance(q, dict) and q.get("usage") is usage_flag:
            return q
    return None

#funzione per scrivere rapidamente le quote, cerco se un campo è presente nelle quote del messaggio e in tal caso la scrivo in un dizionario 
def map_into_fields(src: dict, mapping: list[tuple[str, str]], dest: dict):
    """
    Copia da 'src' a 'dest' i campi indicati in 'mapping' come (key_src, key_dst)
    """
    if not src:
        return
    for key_src, key_dst in mapping:
        if key_src in src:
            dest[key_dst] = int(src[key_src])

# ---------- Pipeline principale ----------
#Il messaggio viene convertito in un dizionario e vengono selezionate le informazioni utili poi salvate in una lista come stringhe
def json_to_lines(msg: dict) -> list[str]:
    lines = []

    # Tag comuni dal root (tutti i LP hanno questi tags)
    tags_comuni = {
        "provider_name": msg.get("provider_name", ""),
        "provider_type": msg.get("provider_type", ""),
        "region_name":   msg.get("region_name", ""),
        "user_group":    msg.get("user_group", ""),
    }

    # Timestamp ns
    ts_ns = parse_ts_ns(msg["timestamp"])

    # ---- Prima table: general_info----
    general_fields = {
         "overbooking_cpu": float(msg.get("overbooking_cpu", -1)),
         "overbooking_ram": float(msg.get("overbooking_ram", -1)),
         "bandwidth_in":    float(msg.get("bandwidth_in", -1)),
         "bandwidth_out":   float(msg.get("bandwidth_out", -1)),
          }

    line = build_line("general_info", tags_comuni, general_fields, ts_ns)
    if line:
        lines.append(line)

    # ---- Seconda table-primo servizio: block_storage_services ----
    bss = msg.get("block_storage_services", [])  # lista con 0 o 1 elemento
    if bss:
        b0 = bss[0]  # unico dizionario del servizio
        tags = tags_comuni | {"type": b0.get("type", "")}
        quotas = b0.get("quotas", [])

        q_quota = find_quota(quotas, usage_flag=False)  # limiti disponibili
        q_usage = find_quota(quotas, usage_flag=True)   # utilizzo corrente

        fields = {}
        map_into_fields(q_quota, [
            ("gigabytes", "storage_gb_quota"), ("volumes", "volume_quota"),], fields)
        map_into_fields(q_usage, [("gigabytes", "storage_gb_usage"), ("volumes", "volume_usage"),], fields)

    line = build_line("block_storage_services", tags, fields, ts_ns)
    if line:
        lines.append(line)

   
   # ---- Terza table-secondo servizio: compute_services ----
    cs = msg.get("compute_services", [])
    if cs:
        c0 = cs[0]
        tags = tags_comuni | {"type": c0.get("type", "")}
        q_quota = find_quota(c0.get("quotas", []), usage_flag=False)  # limiti disponibili
        q_usage = find_quota(c0.get("quotas", []), usage_flag=True)   # utilizzo corrente

        fields = {}

        map_into_fields(q_quota, [("cores", "cores_quota"),
            ("instances", "instances_quota"), ("ram", "ram_quota"),], fields)

        map_into_fields(q_usage, [("cores", "cores_usage"), ("instances", "instances_usage"), ("ram", "ram_usage"),], fields)

        line = build_line("compute_services", tags, fields, ts_ns)
        if line:
            lines.append(line)

        # ---- Quarta table-terzo servizio: network_services ----
        ns = msg.get("network_services", [])  # lista con 0 o 1 elemento
        if ns:  # servizio presente nel messaggio
            n0 = ns[0]
            tags = tags_comuni | {"type": n0.get("type", "")}

            quotas = n0.get("quotas", [])
            q_quota = find_quota(quotas, usage_flag=False)  # limiti disponibili
            q_usage = find_quota(quotas, usage_flag=True)   # utilizzo corrente

            fields = {}

            # quota
            map_into_fields(q_quota, [
                ("networks",              "networks_quota"),
                ("ports",                 "ports_quota"),
                ("public_ips",            "public_ips_quota"),
                ("security_group_rules",  "security_group_rules_quota"),
                ("security_groups",       "security_groups_quota"),
            ], fields)

            # usage  <-- qui prima avevi q_quota per errore
            map_into_fields(q_usage, [
                ("networks",              "networks_usage"),
                ("ports",                 "ports_usage"),
                ("public_ips",            "public_ips_usage"),
                ("security_group_rules",  "security_group_rules_usage"),
                ("security_groups",       "security_groups_usage"),
            ], fields)

            line = build_line("network_services", tags, fields, ts_ns)
            if line:
                lines.append(line)


    # ---- Quinta Table-quarto servizio: object_store_services ----
    oss = msg.get("object_store_services", [])
    if oss:
        o0 = oss[0]
        tags = tags_comuni | {"type": o0.get("type", "")}
        pass

    return lines

if __name__ == "__main__":
    with open("sample.json", "r", encoding="utf-8") as f:
        msg = json.load(f)

    lines = json_to_lines(msg)
    #for ln in lines:
        #print(ln)
    body = "\n".join(lines)

    with open("/home/francesco/progetto_influx/data/input.lp", "w", encoding="utf-8") as f:
      f.write(body)

