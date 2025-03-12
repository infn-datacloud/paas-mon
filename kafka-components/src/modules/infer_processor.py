import pandas as pd # type: ignore
import sys
from modules import infer_processor_conf as ipc
from datetime import datetime, timedelta

prov_data = dict()
rally_msgs = list()

def get_rally_perc_failures(provider_name, days):
    if not isinstance(days,(int,float)):
        raise TypeError("'Days' is not integer or float")

    global rally_msgs
    if len(rally_msgs) > 0:
        df = pd.DataFrame(rally_msgs)
        if provider_name in df[ipc.R_PROVIDER_COL].unique():
            df[ipc.R_TIMESTAMP_COL] = pd.to_datetime(df[ipc.R_TIMESTAMP_COL])
            now = datetime.now() if not ipc.R_TEST else df.timestamp.max()
            df[ipc.R_TEST_RESULT_COL] =  \
                df[ipc.R_TEST_RESULT_COL] == ipc.R_TEST_RESULT_TRUE
            df = df.drop_duplicates()
            df = df[(df[ipc.R_TIMESTAMP_COL] > now - timedelta(days=days)) &
                    (df[ipc.R_STATUS_COL] == ipc.R_STATUS_FINISHED) &                         
                    (df[ipc.R_PROVIDER_COL] == provider_name) ]
            failure = 1 - df[ipc.R_TEST_RESULT_COL].sum() / df.shape[0] 
            failure_perc = round(failure, 3)
            return failure_perc, ""
        else:
            prov_list = list(df[ipc.R_PROVIDER_COL].unique())
            msg = f"No '{provider_name}' in the monitored providers: {prov_list}"
            return None, msg
    return None, "No stored values"
    
def update_providers_data(msg):
    global prov_data
    if not isinstance(msg, dict):
        m = msg.value
        m[ipc.F_TIMESTAMP_KEY] = msg.timestamp
    else:
        m = msg 
        if ipc.PD_TEST_TIMESTAMP in m:
            m[ipc.F_TIMESTAMP_KEY] = m[ipc.PD_TEST_TIMESTAMP]
        else:
            return "No timestamp collected"

    if isinstance(m, dict):
        if ipc.PD_MSG_VERSION not in m:
            return ""
        
        if m[ipc.PD_PROVIDER_NAME_KEY] in ipc.F_PROVIDER_TO_REJECT:
            return ""
        
        if all(key in m for key in ipc.PD_PROVIDER_ID_KEYS):
            str_k = '-'.join([m[k] 
                              for k in ipc.PD_PROVIDER_ID_KEYS])
            if str_k not in prov_data \
               or ipc.F_TIMESTAMP_KEY not in prov_data[str_k] \
               or m[ipc.F_TIMESTAMP_KEY] > prov_data[str_k][ipc.F_TIMESTAMP_KEY]:
                    prov_data[str_k] = m
            return ""
        else:
            return f"Error: Required keys {ipc.PD_PROVIDER_ID_KEYS} not present {m.keys()}"


def import_size(str_value):
    if   ipc.VT_GB_SIZE_SUFFIX in str_value: 
        value = float(str_value.replace(ipc.VT_GB_SIZE_SUFFIX, "")) 
    elif ipc.VT_TB_SIZE_SUFFIX in str_value: 
        value = float(str_value.replace(ipc.VT_TB_SIZE_SUFFIX, "")) * 1_000
    elif ipc.VT_PB_SIZE_SUFFIX in str_value: 
        value = float(str_value.replace(ipc.VT_PB_SIZE_SUFFIX, "")) * 1_000 * 1_000
    else:
        #logging.warning(f"Impossible parse di memory {str_value}")
        value = -1
    return value

def get_compute_info(vt_c_obj: dict) -> dict:
    """vt_c_obj: Validated Template Computing Section Object"""

    info = { item:None for item in ipc.DD_COMP_KEYS }
    if ipc.VT_SCAL_KEY in vt_c_obj and \
       ipc.VT_PROP_KEY in vt_c_obj[ipc.VT_SCAL_KEY] and \
       ipc.VT_COUNT_KEY in vt_c_obj[ipc.VT_SCAL_KEY][ipc.VT_PROP_KEY]:
        info[ipc.DD_INSTANCE_KEY] = vt_c_obj[ipc.VT_SCAL_KEY][ipc.VT_PROP_KEY][ipc.VT_COUNT_KEY]
    if ipc.VT_HOST_KEY in vt_c_obj and \
       ipc.VT_PROP_KEY in vt_c_obj[ipc.VT_HOST_KEY]:
        host_obj = vt_c_obj[ipc.VT_HOST_KEY][ipc.VT_PROP_KEY] 
        info[ipc.DD_VCPUS_KEY] = int(host_obj[ipc.VT_NUM_CPUS])
        info[ipc.DD_RAM_KEY] = import_size(host_obj[ipc.VT_MEM_SIZE_KEY])
        if ipc.VT_DISK_SIZE_KEY in host_obj:
            info[ipc.DD_DISK_KEY] = import_size(host_obj[ipc.VT_DISK_SIZE_KEY])
        if ipc.VT_NUM_GPUS_KEY in host_obj:
            info[ipc.DD_GPUS_KEY] = int(host_obj[ipc.VT_NUM_GPUS_KEY])
        if info[ipc.DD_INSTANCE_KEY] is None: 
            info[ipc.DD_INSTANCE_KEY] = 1
    if ipc.VT_OS_KEY in vt_c_obj and \
       ipc.VT_PROP_KEY in vt_c_obj[ipc.VT_OS_KEY]:
        os_obj = vt_c_obj[ipc.VT_OS_KEY][ipc.VT_PROP_KEY]
        info[ipc.DD_OS_DISTRO_KEY] = str(os_obj[ipc.VT_DISTRIBUTION_KEY])
        info[ipc.DD_OS_VERSION_KEY] = str(os_obj[ipc.VT_VERSION_KEY])
    return info

def get_storage_info(vt_s_obj: dict) -> dict:
    """vt_s_obj = Validated Template Storage Section Object"""
    info = dict()
    if ipc.VT_SIZE_KEY in vt_s_obj:
        info[ipc.DD_STORAGE_KEY] = import_size(vt_s_obj[ipc.VT_SIZE_KEY])
        info[ipc.DD_VOLUMES_KEY] = 1
    return info

# Import data in template
def extract_data_from_valid_template(val_templ: dict) -> dict:
    if "dep_name" in val_templ:
        val_templ['template_name'] = val_templ['dep_name']
        del val_templ['dep_name']
    dep_data = {k:val_templ[k] for k in ipc.VT_KEYS_TO_IMPORT}
    dep_data[ipc.DD_NODE_INFO_KEY] = dict()
    dep_data[ipc.DD_STORAGE_INFO_KEY] = dict()
    dep_data[ipc.DD_PROVIDERS_KEY] = dict()
    top_templ = val_templ[ipc.VT_TOPOLOGY_KEY]
    for node_key, node_obj in top_templ[ipc.VT_NODE_KEY].items():
        if node_obj[ipc.VT_TYPE_KEY] == ipc.VT_BLOCKSTORAGE_TYPE and \
           ipc.VT_PROP_KEY in node_obj:
            vt_s_obj = node_obj[ipc.VT_PROP_KEY]
            dep_data[ipc.DD_STORAGE_INFO_KEY][node_key] = get_storage_info(vt_s_obj)
        elif node_obj[ipc.VT_TYPE_KEY] == ipc.VT_COMPUTER_TYPE and \
             ipc.VT_CAPABILITIES_KEY in node_obj:
            vt_c_obj = node_obj[ipc.VT_CAPABILITIES_KEY]
            dep_data[ipc.DD_NODE_INFO_KEY][node_key] = get_compute_info(vt_c_obj)
    for node_key, node_obj in top_templ[ipc.VT_OUTPUTS_KEY].items():
        if ipc.VT_IP_SUFFIX in node_key:
            if ipc.VT_VALUE_KEY in node_obj and \
               ipc.VT_GET_ATTR_KEY in node_obj[ipc.VT_VALUE_KEY] and \
               len(node_obj[ipc.VT_VALUE_KEY][ipc.VT_GET_ATTR_KEY]) > 1 and \
               ipc.VT_PUB_ADDR_KEY in node_obj[ipc.VT_VALUE_KEY][ipc.VT_GET_ATTR_KEY][1]:
                node_name = node_obj[ipc.VT_VALUE_KEY][ipc.VT_GET_ATTR_KEY][0]
                if node_name in dep_data[ipc.DD_NODE_INFO_KEY]:
                    dep_data[ipc.DD_NODE_INFO_KEY][node_name][ipc.DD_FLOATINGIPS_KEY] = 1
    return dep_data

def get_info_from_provider(obj: dict) -> dict:
    info = {ipc.DD_IMAGES_KEY: False,
            ipc.DD_QUOTAS_KEY: dict(),
            ipc.DD_BEST_FLAVOR_KEY: dict()
           }   
    
    info.update(
        {dd_key:obj[pd_key] for dd_key, pd_key in ipc.BASIC_PROV_DATA_INFO if pd_key in obj}
    )
    return info

def create_key(base: str, usage: bool) -> str:
    return (base + ipc.DD_USAGE_SUFFIX) if usage else (base + ipc.DD_QUOTA_SUFFIX)

def collect_quotas(prov_data: dict) -> dict:
    quotas = dict()
    for prov_key,prov_obj in prov_data.items():
        if prov_key in ipc.M_QUOTAS_KEYS:
            for obj in prov_obj:
                for q_elem in obj[ipc.PD_QUOTAS_KEY]:
                    for dd_key, pd_key, factor in ipc.M_QUOTAS_KEYS[prov_key]:
                        obj_key = create_key(dd_key, q_elem[ipc.PD_USAGE_KEY])
                        obj_value = int(q_elem[pd_key]/factor)
                        quotas[obj_key] = obj_value
    return quotas

def is_param_inclused(dn_param, pf_param):
    return dn_param <= pf_param if dn_param is not None and pf_param is not None else True

def is_param_equal(dn_param, pf_param):
    return dn_param == pf_param if dn_param is not None and pf_param is not None else True

def are_params_inclused(n_obj, f_obj):
    return all([is_param_inclused(n_obj[dn_key], f_obj[pf_key] / f)
               for dn_key, pf_key, f in ipc.MISC_BEST_MATCHER_KEYS])

def are_params_equal(n_obj, f_obj):
    return all([is_param_equal(n_obj[dn_key], f_obj[pf_key] / f)
               for dn_key, pf_key, f in ipc.MISC_BEST_MATCHER_KEYS])

def get_best_flavor(node_obj, prov_flavors):
    best_flavor = {k:sys.maxsize for k in ipc.DD_FLAVOR_KEYS}
    for prov_flavor in prov_flavors:
        if are_params_inclused(node_obj, prov_flavor):
            if all([prov_flavor[pf_key]/f <= best_flavor[dn_key] 
                    for dn_key, pf_key, f in ipc.MISC_BEST_MATCHER_KEYS]):
                best_flavor = {dn_key: prov_flavor[pf_key]/f 
                               for dn_key, pf_key, f in ipc.MISC_BEST_MATCHER_KEYS}
                best_flavor[ipc.DD_EXACT_FLAVOR] = are_params_equal(node_obj, prov_flavor)
    temp = node_obj.copy()
    temp.update(best_flavor)
    return temp

#Find is all needed images for the deployments are available in the given cloud project
def check_single_image(node,image):
    return node[ipc.DD_OS_DISTRO_KEY]  == image[ipc.PD_OS_DISTRO_KEY] and \
           node[ipc.DD_OS_VERSION_KEY] == image[ipc.PD_OS_VERSION_KEY]

def is_image_present(node,data):
    images = data[ipc.PD_COMPUTE_SERVICES_KEY][0][ipc.PD_IMAGES_KEY]
    return any(check_single_image(node,image) for image in images)

def are_present_images(dep_data, prov_data):
    nodes = dep_data[ipc.DD_NODE_INFO_KEY].values() 
    return all(is_image_present(node,prov_data) for node in nodes
               if node[ipc.DD_INSTANCE_KEY] > 0)

def best_match_finder(dep_data):
    global prov_data
    for p_key, p_obj in prov_data.items():
        if p_obj[ipc.PD_USER_GROUP_KEY] != dep_data[ipc.DD_USER_GROUP_KEY]: 
            continue
   
        info = get_info_from_provider(p_obj)
        info[ipc.DD_QUOTAS_KEY] = collect_quotas(p_obj)
        info[ipc.DD_IMAGES_KEY] = are_present_images(dep_data, p_obj)
        
        for n_name, n_obj in dep_data[ipc.DD_NODE_INFO_KEY].items():
            if n_obj[ipc.DD_INSTANCE_KEY] < 1: 
                continue
            p_flavors = p_obj[ipc.PD_COMPUTE_SERVICES_KEY][0][ipc.PD_FLAVORS_KEY]
            info[ipc.DD_BEST_FLAVOR_KEY][n_name] = get_best_flavor(n_obj, p_flavors)
            dep_data[ipc.DD_PROVIDERS_KEY][p_key] = info
    return dep_data

def remove_null(dep_data):
    for _, prov_data in dep_data[ipc.DD_PROVIDERS_KEY].items():
        for flavor_obj in prov_data[ipc.DD_BEST_FLAVOR_KEY].values():
            for flavor_key, flavor_value in flavor_obj.items():
                if flavor_value is None:
                    flavor_obj[flavor_key] = 0
    return dep_data

def get_rally_provider_name(prov_data):
    keys_to_use = [ipc.PD_PROVIDER_NAME_KEY, 
                   ipc.PD_REGION_NAME_KEY]
    str_key = '-'.join([prov_data[k] for k in keys_to_use])
    return ipc.RALLY_PROVIDER_NAME_MAPPING[str_key]

def compute_aggregated_resource(data):
    for dep_prov_obj in data[ipc.DD_PROVIDERS_KEY].values():
        aggr_res = dict()
        nodes = dep_prov_obj[ipc.DD_BEST_FLAVOR_KEY].values()
        for fl_key in sorted(ipc.DD_AGGR_KEYS):
            acc_value = sum(node[fl_key] * node[ipc.DD_INSTANCE_KEY] 
                            for node in nodes 
                            if node[ipc.DD_INSTANCE_KEY] > 0)
            aggr_res[fl_key + ipc.DD_REQUIRED_SUFFIX] = acc_value
        acc_value = sum([node[ipc.DD_INSTANCE_KEY] for node in nodes])
        aggr_res[ipc.DD_INSTANCE_KEY + ipc.DD_REQUIRED_SUFFIX] = acc_value
        dep_prov_obj[ipc.DD_AGGREGATED_RESOURCES_KEY] = aggr_res

        rally_prov_name = get_rally_provider_name(dep_prov_obj)
        for n_days in ipc.DD_RALLY_N_DAYS:
            rally_value, err = get_rally_perc_failures(rally_prov_name, n_days)
            rally_key = f"{ipc.DD_RALLY_VALUE_KEY}_{n_days}{ipc.DD_RALLY_DAY}"
            dep_prov_obj[rally_key] = -1 if err else rally_value
        

    data[ipc.DD_AGGREGATED_STORAGE_KEY] = dict()
    for stor_key in ipc.DD_STORAGE_SEC_KEYS:
        dep_stor_key = stor_key + ipc.DD_REQUIRED_SUFFIX
        dep_storage_obj = data[ipc.DD_STORAGE_INFO_KEY].values()
        acc_value = sum([storage_value.get(stor_key,0) 
                         for storage_value in dep_storage_obj])
        data[ipc.DD_AGGREGATED_STORAGE_KEY][dep_stor_key] = acc_value
    return data

def get_msg(dep_data: dict) -> dict:
    m = {o_k:dep_data[dd_k] for o_k, dd_k in ipc.O_DD_KEYS_TO_IMPORT} 
    m[ipc.O_MSG_VERSION] = ipc.O_MSG_VERSION_VALUE
    m[ipc.O_PROVIDERS_KEY] = list()
    for provider in dep_data[ipc.DD_PROVIDERS_KEY].values():
        p_info = provider[ipc.DD_AGGREGATED_RESOURCES_KEY]
        p_info[ipc.DD_EXACT_FLAVORS] = p_info[ipc.DD_EXACT_FLAVOR + ipc.DD_REQUIRED_SUFFIX]
        del p_info[ipc.DD_DISK_KEY + ipc.DD_REQUIRED_SUFFIX]
        del p_info[ipc.DD_EXACT_FLAVOR + ipc.DD_REQUIRED_SUFFIX]
        for o_key,dd_key in ipc.O_PROV_DATA:
            if dd_key in provider:
                p_info[o_key] = provider[dd_key]
        # p_info[ipc.O_IMAGES_KEY] = provider[ipc.DD_IMAGES_KEY]
        for k in provider:
            if ipc.DD_RALLY_VALUE_KEY in k:
                nk = ipc.O_TEST_FAILURE_PERC + k.split(ipc.DD_RALLY_VALUE_KEY)[1]
                p_info[nk] = provider[k]
        p_info.update(provider[ipc.DD_QUOTAS_KEY])
        p_info.update(dep_data[ipc.DD_AGGREGATED_STORAGE_KEY])
        for k,v in p_info.items():
            if isinstance(v,int): 
                p_info[k] = float(v)
        m[ipc.O_PROVIDERS_KEY].append(p_info)
    return m