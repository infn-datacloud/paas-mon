import yaml
from datetime import datetime
import json
import orc_log_processor_conf as olc

def get_uuid(templ):
    return templ[olc.TEMPL_UUID]

def is_start_to_collect(ts, line):
    return ts and olc.ORC_LOG_START_TEMPLATE_STRING in line

def is_line_to_reject(line):
    return olc.ORC_LOG_FILTER not in line
    #return olc.ORC_LOG_FILTER not in line or olc.ORC_LOG_SEP not in line

def is_template_meta_data(line):
    return olc.ORC_LOG_EVENT_TEMPLATE_SEP in line and olc.ORC_LOG_INFO_TEMPLATE_STRING in line

def add_timestamp(template, ts):
    template[olc.TEMPL_TIMESTAMP] = ts.strftime(olc.ORC_TS_FORMAT)
    return template

def extract_info(line):
    syslog_ts =  datetime.strptime(line.split()[0], olc.SYSLOG_TS_FORMAT)
    pre_line = " ".join(line.split()[:3]) + " "
    line = line.split(pre_line)[1]
    try:
        orc_ts_str = " ".join(line.split()[0:2])
        orc_ts = datetime.strptime(orc_ts_str, olc.ORC_TS_FORMAT)
    except (ValueError, IndexError):
        orc_ts = None
    return syslog_ts, orc_ts, line

# Parse timestamp
def extract_timestamp(line):
    syslog_ts = datetime.strptime(line.split()[0], olc.SYSLOG_TS_FORMAT)
    try:
        orc_ts_str = " ".join(line.split(olc.ORC_LOG_SEP)[1].split()[0:2])
        orc_ts = datetime.strptime(orc_ts_str, olc.ORC_TS_FORMAT)
    except (ValueError, IndexError):
        orc_ts = None
    return syslog_ts, orc_ts

def extract_info2(line):
    return str(line).split(olc.ORC_LOG_SEP)[1]


# Validate YAML
def import_template(str_template):
    try:
        return (yaml.safe_load("\n".join(str_template)), True)
    except Exception:
        return (dict(), False)

# Extract user parameter from json
def extract_user_parameters(line: str) -> dict:
    str_json = line.split(olc.ORC_LOG_EVENT_TEMPLATE_SEP)[1]
    try:
        return json.loads(str_json)
    except Exception:
        return None

# Collect template_name, uuid and user_group from template and deploment parameters:
def get_basic_info_template(templ, depl_data):
    val_templ = templ.copy()
    val_templ[olc.TEMPL_MSG_VERSION] = olc.TEMPL_MSG_VERSION_VALUE
    val_templ[olc.TEMPL_TEMPL_NAME] = None 
    if olc.TEMPL_METADATA in templ and olc.TEMPL_DISPLAY_NAME in templ[olc.TEMPL_METADATA]:
        val_templ[olc.TEMPL_TEMPL_NAME] = templ[olc.TEMPL_METADATA][olc.TEMPL_DISPLAY_NAME]
    elif olc.TEMPL_DESCRIPTION in templ:
        if olc.TEMPL_DESCRIPTION_K8S in str(templ[olc.TEMPL_DESCRIPTION]).lower():
            val_templ[olc.TEMPL_TEMPL_NAME] = olc.TEMPL_DESCRIPTION_K8S_VALUE
        else:
            val_templ[olc.TEMPL_TEMPL_NAME] = templ[olc.TEMPL_DESCRIPTION]
    val_templ[olc.TEMPL_USER_GROUP] = depl_data.get(olc.TEMPL_USER_GROUP, None)
    val_templ[olc.TEMPL_UUID] = depl_data.get(olc.TEMPL_UUID, None)
    return val_templ

# Extract parameter type from template 
def get_param_type(param_obj):
    if olc.TYPE_FIELD in param_obj:
        param_type = param_obj[olc.TYPE_FIELD]
        if param_type in olc.TYPES_TO_INTEGER: 
            return int
        elif param_type in olc.TYPES_TO_STRING: 
            return str
    return None

def cast_param(param, param_type):
    return param_type(param) if param_type else param

def get_error_user_not_in_valids(user_parameter, valid_values):
    msg =  f"{user_parameter=}({type(user_parameter)=}) not "
    msg += f"in {valid_values=}({type(valid_values[0])=})"
    return msg

def get_default_value(param_obj: dict) -> bool:
    return olc.PARAM_DEFAULT in param_obj

def get_required_value(param_obj: dict) -> bool | None:
    return param_obj.get(olc.PARAM_REQUIRED, None)

# Check constraints, default and value type
def get_param(param_obj, user_parameter, use_constraints=False):
    
    # Extract parameter type from template
    param_type = get_param_type(param_obj)
    
    # If use did not inserted the parameter
    if not user_parameter:

        param_default = get_default_value(param_obj)
        param_required = get_required_value(param_obj)

        if param_default == False and param_required == False: # Suggestion to not approve
            return None, olc.MSG_PARAM_ERROR1 
        elif param_default == False or param_required == True: # Suggestion to not approve
            return None, olc.MSG_PARAM_ERROR2 
        else:
            # Consider default value
            value = cast_param(param_obj[olc.PARAM_DEFAULT], param_type)
            return value, olc.MSG_NO_ERR

    # If here, user provided a parameter

    # Cast the value to the type stated in the template
    user_parameter = cast_param(user_parameter, param_type)

    if use_constraints:
        valid_values = None         # Init valid_values list

        # Collect "valid_values" field
        for constr_el in param_obj[olc.PARAM_CONSTRAINTS]:
            for constr_k, constr_obj in constr_el.items():
                if constr_k == olc.PARAM_VALID_VALUES:
                    valid_values = [cast_param(elem, param_type) for elem in constr_obj]

        if valid_values:
            if user_parameter in valid_values:
                # If the user parameter belongs to "valid_values" list, returns it
                return user_parameter, olc.MSG_NO_ERR
            else:
                # If not, report
                return None, get_error_user_not_in_valids(user_parameter, valid_values)
        else:
            return user_parameter, olc.MSG_NO_ERR
            # If here, the user parameter is provided and the checking on constraints must be done
            # but no "valid_values" list has been imported. To report.
        #    msg = f"{olc.MSG_PARAM_ERROR3} {param_obj=}"
        #    return None, msg
    else:
        # No constrain 
        return user_parameter, olc.MSG_NO_ERR

# Merge user parameters and template defualt and requirements
def get_val_templ(template, depl_data):
    
    template = get_basic_info_template(template, depl_data)
    # Validate and merge parameters (user and default parameters, constraints and required)
    for param_key, param_obj in template[olc.TEMPL_TOPOLOGY_TEMPL][olc.TEMLP_INPUTS].items():
        user_param = depl_data[olc.INPUT_USER_PARAMS].get(param_key, None)
        param_to_check = param_key not in olc.PARAM_TO_NOT_CHECK
        to_constraint = olc.USE_CONSTRAINTS and olc.PARAM_CONSTRAINTS in param_obj and param_to_check
        param, err_msg = get_param(param_obj, user_param, to_constraint)
        if err_msg:
            # Forward message error outside
            msg = f"Error during the validation of '{param_key}' parameter. Message: {err_msg}"
            template = '{}'
            return template, msg
        else:
            template[olc.TEMPL_TOPOLOGY_TEMPL][olc.TEMLP_INPUTS][param_key] = param

    def find_get_input(var):
        if isinstance(var, dict):
            for k,v in var.items():
                if isinstance(v,dict) and olc.TEMPL_GET_INPUT in v:
                    input_var = v[olc.TEMPL_GET_INPUT]
                    input_value = template[olc.TEMPL_TOPOLOGY_TEMPL][olc.TEMLP_INPUTS][input_var]
                    var[k] = input_value
                else:
                    find_get_input(v)
        if isinstance(var,list):
            for el in var:
                find_get_input(el)
        
    for _, node_data in template[olc.TEMPL_TOPOLOGY_TEMPL][olc.TEMPL_NODE_TEMPL].items():
        find_get_input(node_data)
                                    
    err_msg = ""
    str_template = json.dumps(template, sort_keys=True)
    return str_template, err_msg