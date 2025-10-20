from datetime import datetime

# Converts an ISO 8601 timestamp string to a Unix epoch timestamp in nanoseconds (UTC)
def parse_ts_ns(ts_str: str) -> int:
    """
    Parse an ISO 8601 formatted timestamp string and convert it into a Unix epoch timestamp expressed in nanoseconds (integer).

    Args:
        ts_str (str): The timestamp string in ISO 8601 format (e.g., '2025-10-03T07:05:33+00:00'), including timezone offset.

    Returns:
        int: The corresponding Unix epoch timestamp in nanoseconds (UTC-based).
    """
    if ts_str.endswith("Z"):
        ts_str = ts_str[:-1] + "+00:00"

    dt = datetime.fromisoformat(ts_str)
    return int(dt.timestamp() * 1_000_000_000)

# Escapes special characters in strings to make them valid in InfluxDB Line Protocol.
# InfluxDB syntax requires escaping commas, spaces, and equal signs
# within measurement names, tag keys/values, and field keys: <measurement>,<tag_key>=<tag_val> <field_key>=<field_val> <timestamp>
def escape_measurement(m: str) -> str:
    return m.replace(",", r"\,").replace(" ", r"\ ")

def escape_tag_key(k: str) -> str:
    return k.replace(",", r"\,").replace(" ", r"\ ").replace("=", r"\=")

def escape_tag_val(v: str) -> str:
    return str(v).replace(",", r"\,").replace(" ", r"\ ").replace("=", r"\=")

def escape_field_key(k: str) -> str:
    return k.replace(",", r"\,").replace(" ", r"\ ").replace("=", r"\=")
def format_field_val(v):
    """
    Format a Python value into a valid InfluxDB Line Protocol field value.

    Conversion rules:
        - int   → '123i'        (integers must end with 'i')
        - float → '123.45'      (decimal point notation)
        - bool  → 'true' / 'false'
        - str   → "text"        (enclosed in double quotes, with escaped quotes inside)
    """
    if isinstance(v, bool):
        return "true" if v else "false"
    if isinstance(v, int):
        return f"{v}i"
    if isinstance(v, float):
        return repr(v)
    # String: escape any internal double quotes
    s = str(v).replace('"', r'\"')
    return f"\"{s}\""

# Builds a valid InfluxDB Line Protocol string.
# If no fields are provided (empty dict), returns an empty string because InfluxDB requires at least one field per line.
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
        
# In the message, 'quotas' is a key within the measurement dictionary.
# Its value is a list containing two elements: one describing used resources ("usage": true) and one describing available resources ("usage": false).
# This function checks whether 'quotas' exists and is a list, then returns  the dictionary corresponding to either used or available resources.
def find_quota(quotas: list, usage_flag: bool):
    if not isinstance(quotas, list):
        return None
    for q in quotas:
        if isinstance(q, dict) and q.get("usage") is usage_flag:
            return q
    return None

# Utility function to copy selected quota fields from a source dictionary into a destination dictionary.
def map_into_fields(src: dict, mapping: list[tuple[str, str]], dest: dict):
    """
    Copia da 'src' a 'dest' i campi indicati in 'mapping' come (key_src, key_dst)
    """
    if not src:
        return
    for key_src, key_dst in mapping:
        if key_src in src:
            dest[key_dst] = src[key_src]

# ---------- Main Conversion Pipeline ----------
# The incoming JSON message is parsed into a Python dictionary.
# Relevant information is extracted and formatted as a list of InfluxDB
def json_to_lines(msg: dict) -> list[str]:
    version = msg.get("msg_version")
    if version != "1.3.0":
        raise ValueError(f"Unsupported msg_version: {version}")

    lines = []

    # Tags shared across all measurements
    tags_comuni = {
        "provider_name": msg.get("provider_name", ""),
        "provider_type": msg.get("provider_type", ""),
        "region_name":   msg.get("region_name", ""),
        "user_group":    msg.get("user_group", ""),
    }

    # Timestamp ns
    ts_ns = parse_ts_ns(msg["timestamp"])

    # ---- General_info----
    general_fields = {
         "overbooking_cpu": float(msg.get("overbooking_cpu", -1)),
         "overbooking_ram": float(msg.get("overbooking_ram", -1)),
         "bandwidth_in":    float(msg.get("bandwidth_in", -1)),
         "bandwidth_out":   float(msg.get("bandwidth_out", -1)),
          }

    line = build_line("general_info", tags_comuni, general_fields, ts_ns)
    if line:
        lines.append(line)

    # ---- Block_storage_services ----
    bss = msg.get("block_storage_services", [])  
    if bss:
        b0 = bss[0]  
        tags = tags_comuni | {"type": b0.get("type", "")} 
        quotas = b0.get("quotas", [])

        q_quota = find_quota(quotas, usage_flag=False)  # available limits
        q_usage = find_quota(quotas, usage_flag=True)   # current usage

        fields = {}
        map_into_fields(q_quota, [
            ("gigabytes", "storage_gb_quota"), ("volumes", "volume_quota"),], fields)
        map_into_fields(q_usage, [("gigabytes", "storage_gb_usage"), ("volumes", "volume_usage"),], fields)

        line = build_line("block_storage_services", tags, fields, ts_ns)
        if line:
            lines.append(line)

   
   # ---- Compute_services ----
    cs = msg.get("compute_services", [])
    if cs:
        c0 = cs[0]
        tags = tags_comuni | {"type": c0.get("type", "")}
        q_quota = find_quota(c0.get("quotas", []), usage_flag=False)  # available limits
        q_usage = find_quota(c0.get("quotas", []), usage_flag=True)   # current usage

        fields = {}

        map_into_fields(q_quota, [("cores", "cores_quota"),
            ("instances", "instances_quota"), ("ram", "ram_quota"),], fields)

        map_into_fields(q_usage, [("cores", "cores_usage"), ("instances", "instances_usage"), ("ram", "ram_usage"),], fields)

        line = build_line("compute_services", tags, fields, ts_ns)
        if line:
            lines.append(line)

    # ---- Network_services ----
    ns = msg.get("network_services", [])  
    if ns:  
         n0 = ns[0]
         tags = tags_comuni | {"type": n0.get("type", "")}

         quotas = n0.get("quotas", [])
         q_quota = find_quota(quotas, usage_flag=False)  # available limits
         q_usage = find_quota(quotas, usage_flag=True)   # current usage

         fields = {}

         
         map_into_fields(q_quota, [
                ("networks",              "networks_quota"),
                ("ports",                 "ports_quota"),
                ("public_ips",            "public_ips_quota"),
                ("security_group_rules",  "security_group_rules_quota"),
                ("security_groups",       "security_groups_quota"),
            ], fields)

           
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


    # ---- Object_store_services ----
    oss = msg.get("object_store_services", [])
    if oss:
        o0 = oss[0]
        tags = tags_comuni | {"type": o0.get("type", "")}
        pass

    return lines
