# /bin/env python3

# Python dependecies:
# - kafka-python

import json
from datetime import datetime
import yaml
from kafka import KafkaConsumer, KafkaProducer
import string
import random
import os 

# Kafka parameteres
kafka_log_orchestrator_envvar =          'TEMPLATE_PARSER_KAFKA_LOG_ORCHESTRATOR_TOPIC'
kafka_validated_tamplte_topic_envvar =   'TEMPLATE_PARSER_KAFKA_VAL_TEMPL_TOPIC'
kafka_log_app_topic_envvar =             'TEMPLATE_PARSER_KAFKA_LOG_APP_TOPIC'
kakfa_bootstrap_servers_envvar =         'TEMPLATE_PARSER_KAFKA_BOOTSTRAP_SERVERS'

kafka_log_orchestrator_topic_default =   'test'
kafka_validated_template_topic_default = 'validated-templates'
kafka_log_app_topic_default =            'logs-parser-templates'
kafka_bootstrap_servers_default =        "192.168.21.96:9092"

input_topic = os.environ.get(kafka_log_orchestrator_envvar, kafka_log_orchestrator_topic_default)
val_templ_topic = os.environ.get(kafka_validated_tamplte_topic_envvar, kafka_validated_template_topic_default)
log_topic = os.environ.get(kafka_log_app_topic_envvar, kafka_log_app_topic_default)
bootstrap_servers = os.environ.get(kakfa_bootstrap_servers_envvar, kafka_bootstrap_servers_default).split(',')
base_group_id_name = "template-parser"

# App parameters
USE_CONSTRAINTS = True
app_string_splitter = "]: "
event_template_string = "i.r.o.service.DeploymentServiceImpl      : " 
start_template_string = "Creating deployment with template"
info_template_string = "{\"uuid\"" 
syslog_ts_format = "%Y-%m-%dT%H:%M:%S%z"  # YYYY-MM-DD HH:MM:SS+ZZ:ZZ
app_ts_format = "%Y-%m-%d %H:%M:%S.%f"  # YYYY-MM-DD HH:MM:SS
string_filter = "orchestrator orchestrator"

# Write message in kafka topic
def write_msg_to_kafka(data, topic):
    producer = KafkaProducer(bootstrap_servers=bootstrap_servers,
                             value_serializer=lambda x: json.dumps(x, sort_keys=True).encode('utf-8'))
    if isinstance(data, list):
        for msg in data:
            producer.send(topic, msg)
    else:
        producer.send(topic, data)
    producer.flush()
    producer.close()

# write log in kafka
def write_log_to_kafka(data):
    write_msg_to_kafka(data, log_topic)

# write validated template in kafka
def write_val_templ_kakfa(data):
    write_msg_to_kafka(data, val_templ_topic)

def write_log(str_ts, uuid, status, msg):
    log = dict()
    log['timestamp'] = str_ts.strftime(app_ts_format)
    log['uuid'] = uuid
    log['msg'] = msg
    log['status'] = status
    write_log_to_kafka(log)
    
def get_validated_templates():
    group_id = ''.join(random.choices(string.ascii_uppercase +
                                      string.ascii_lowercase +
                                      string.digits, k=64))
    consumer = KafkaConsumer(
        val_templ_topic,
        bootstrap_servers = bootstrap_servers,
        group_id = f'{base_group_id_name}-{group_id}',
        auto_offset_reset = 'earliest', 
        enable_auto_commit = True,
        value_deserializer = lambda x: x.decode('utf-8'),
        max_partition_fetch_bytes=100_000_000,
        fetch_max_bytes = 50_000_000,
        consumer_timeout_ms=1000
    )

    return [message.value for message in consumer]

# Parse timestamp
def extract_timestamp(line):
    syslog_ts = datetime.strptime(line.split()[0], syslog_ts_format)
    try:
        app_ts_str = " ".join(line.split(']: ')[1].split()[0:2])
        app_ts = datetime.strptime(app_ts_str, app_ts_format)
    except ValueError:
        app_ts = None
    return syslog_ts, app_ts

# Validate YAML
def import_template(str_template):
    try:
        return (yaml.safe_load("\n".join(str_template)), True)
    except Exception:
        return (dict(), False)

# Extract parameter type from template 
def get_param_type(param_obj):
    if "type" in param_obj:
        param_type = param_obj['type']
        if param_type == "integer": 
            return int
        elif param_type in ["scalar-unit.size", 'string', 'version']: 
            return str
    return None

# Extract user parameter from json
def extract_user_parameters(str_json: str) -> dict:
    data = json.loads(str_json)
    return data

# Collect template_name, uuid and user_group from template and deploment parameters:
def get_basic_info_template(template, depl_data):
    validated_template = template.copy()
    validated_template['template_name'] = None 
    if 'metadata' in template and 'display_name' in template['metadata']:
        validated_template['template_name'] = template['metadata']['display_name']
    elif 'description' in template:
        if "kubernetes" in str(template['description']).lower():
            validated_template['template_name'] = "Cluster Kubernetes"
        else:
            validated_template['template_name'] = template['description']
    validated_template['user_group'] = depl_data['user_group'] if 'user_group' in depl_data else None
    validated_template['uuid'] = depl_data['uuid'] if 'uuid' in depl_data else None
    return validated_template

def cast_param(param, param_type):
    return param_type(param) if param_type else param

# Check constraints, default and value type
def get_param(param_obj, user_parameter, use_constraints=False):
    
    # Extract parameter type from template
    param_type = get_param_type(param_obj)

    # If use did not inserted the parameter
    if not user_parameter:

        # If default parameter is provided use that
        if 'default' in param_obj:
            return cast_param(param_obj['default'],param_type), ""
        
        # If here, no user or default parameters are provided
        # So an error will be thrown
        else:
            if 'required' in param_obj:
                if isinstance(param_obj['required'], bool):
                    if param_obj['required'] == True:
                        msg = "Parameter required and not default and user parameter provided"
                        return None, msg
                    else:
                        msg = "Parameter not required and not default and user parameter provided"
                        return None, msg
                else:
                    msg = f"Required parameter not boolean: ({type(param_obj['required'])}){param_obj['required']=}"
                    return None, msg
            else:
                msg = "'required' field, default and user parameter provided "
                return None, msg

    # If here, user provided a parameter

    # Cast the value to the type stated in the template
    user_parameter = cast_param(user_parameter, param_type)
    if use_constraints:

        # Init valid_values list
        valid_values = None

        # Collect "valid_values" field
        for constr_el in param_obj['constraints']:
            for constr_k,constr_obj in constr_el.items():
                if constr_k == "valid_values":
                    valid_values = [cast_param(elem, param_type) for elem in constr_obj]
        
        if valid_values:
            if user_parameter in valid_values:
                # If the user parameter belongs to "valid_values" list, returns it
                return user_parameter, ""
            else:
                # If not, report
                msg = f"{user_parameter=}({type(user_parameter)=}) not in {valid_values=}({type(valid_values[0])=})"
                return None, msg
        else:
            # If here, the user parameter is provided and the check on constraints must be done
            # but no "valid_values" list has been imported. To report.
            msg = f"No contraint collected {param_obj=}"
            return None, msg
    else:
        # No constrain 
        return user_parameter, ""
        
# Merge user parameters and template defualt and requirements
def get_validated_template(template, depl_data):
    template = get_basic_info_template(template, depl_data)
    # Validate and merge parameters (user and default parameters, constraints and required)
    for param_key, param_obj in template['topology_template']['inputs'].items():
        user_param = depl_data['user_parameters'].get(param_key, None)
        to_constraint = USE_CONSTRAINTS and 'constraints' in param_obj and param_key not in ['num_cpus','mem_size']
        param, err_msg = get_param(param_obj, user_param, to_constraint)
        if err_msg:
            # Forward message error outside
            msg = f"Error during the validation of {param_key} parameter. Message: {err_msg}"
            template = {}
            return template, msg
        else:
            template['topology_template']['inputs'][param_key] = param

    def find_get_input(var):
        if isinstance(var, dict):
            for k,v in var.items():
                if isinstance(v,dict) and "get_input" in v:
                    input_var = v['get_input']
                    input_value = template['topology_template']['inputs'][input_var]
                    var[k] = input_value
                else:
                    find_get_input(v)
        if isinstance(var,list):
            for el in var:
                find_get_input(el)
        
    for _, node_data in template['topology_template']['node_templates'].items():
        find_get_input(node_data)
                                    
    err_msg = ""
    return template, err_msg


# Collect already written validated templates
validated_templates = get_validated_templates()

# Init and start Kafka consumer
group_id = ''.join(random.choices(string.ascii_uppercase +
                                  string.ascii_lowercase +
                                  string.digits, k=64))
consumer = KafkaConsumer(
    input_topic,
    bootstrap_servers = bootstrap_servers,
    group_id = f'{base_group_id_name}-{group_id}',
    auto_offset_reset = 'earliest', 
    enable_auto_commit = True,
    value_deserializer = lambda x: x.decode('utf-8'),
#    consumer_timeout_ms = 500
)

collect_template = False
str_template = list()
for message in consumer:
    if not string_filter in message.value: continue
    kafka_log = datetime.fromtimestamp(float(message.timestamp)/1000)
    log_ts, ts = extract_timestamp(message.value)
    line = str(message.value).split(app_string_splitter)[1]
    if ts and start_template_string in line:
        collect_template = True
        continue
    
    if ts and collect_template:
        collect_template = False

    if not ts and collect_template:
        str_template.append(line) 
    
    if event_template_string in line and info_template_string in line:
        str_json = line.split(event_template_string)[1]
        depl_data = extract_user_parameters(str_json)
        template, is_template = import_template(str_template)  
        template['timestamp'] = log_ts.strftime(app_ts_format)
        if is_template:
            validated_template, err_msg = get_validated_template(template, depl_data)
            if not err_msg:
                str_val_templ = json.dumps(validated_template, sort_keys=True)
                if str_val_templ not in validated_templates:
                    write_val_templ_kakfa(validated_template)
                    write_log(log_ts, depl_data['uuid'], "ok", "validated")
                else:
                    write_log(log_ts, depl_data['uuid'], "ok", "validated and template already present")
            else:
                write_log(log_ts, depl_data['uuid'], "nok", err_msg)
        str_template = list()