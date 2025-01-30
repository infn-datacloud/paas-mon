import pandas as pd # type: ignore
import sys
from modules.infer_processor_conf import *
from datetime import datetime, timedelta

prov_data = dict()
rally_msgs = list()

def get_rally_perc_failures(provider_name):
    global rally_msgs
    if len(rally_msgs) > 0:
        df = pd.DataFrame(rally_msgs)
        if provider_name in df[R_PROVIDER_COL].unique():
            df[R_TIMESTAMP_COL] = pd.to_datetime(df[R_TIMESTAMP_COL])
            df[R_TEST_RESULT_COL] =  \
                df[R_TEST_RESULT_COL] == R_TEST_RESULT_TRUE
            df = df.drop_duplicates()
            df = df[(df[R_TIMESTAMP_COL] > datetime.now() - timedelta(days=30)) &
                    (df[R_STATUS_COL] == R_STATUS_FINISHED) &                         
                    (df[R_PROVIDER_COL] == provider_name) ]
            failure = 1 - df[R_TEST_RESULT_COL].sum()  / df.shape[0] 
            failure_perc = round(100 * failure, 1)
            return failure_perc, ""
        else:
            prov_list = list(df[R_PROVIDER_COL].unique())
            msg = f"No '{provider_name}' in the monitored providers: {prov_list}"
            return None, msg
    return None, "No stored values"
    
def update_providers_data(msg: dict):
    global prov_data
    m = msg.value
    ts = msg.timestamp
    m[F_TIMESTAMP_KEY] = ts
    if isinstance(m, dict):
        if all(key in m for key in PD_PROVIDER_ID_KEYS):
            str_k = '-'.join([m[k] 
                              for k in PD_PROVIDER_ID_KEYS])
            if str_k not in prov_data \
               or F_TIMESTAMP_KEY not in prov_data[str_k] \
               or ts > prov_data[str_k][F_TIMESTAMP_KEY]:
                    prov_data[str_k] = m
        else:
            print("Error: Required keys not present")


def import_size(str_value):
    if   VT_GB_SIZE_SUFFIX in str_value: 
        int_value = int(str_value.replace(VT_GB_SIZE_SUFFIX, "")) 
    elif VT_TB_SIZE_SUFFIX in str_value: 
        int_value = int(str_value.replace(VT_TB_SIZE_SUFFIX, "")) * 1_000
    elif VT_PB_SIZE_SUFFIX in str_value: 
        int_value = int(str_value.replace(VT_PB_SIZE_SUFFIX, "")) * 1_000 * 1_000
    else:
        #logging.warning(f"Impossible parse di memory {str_value}")
        int_value = -1
    return int_value

def get_compute_info(vt_c_obj: dict) -> dict:
    """vt_c_obj: Validated Template Computing Section Object"""

    info = { item:None for item in DD_COMP_KEYS }
    if VT_SCAL_KEY in vt_c_obj and \
       VT_PROP_KEY in vt_c_obj[VT_SCAL_KEY] and \
       VT_COUNT_KEY in vt_c_obj[VT_SCAL_KEY][VT_PROP_KEY]:
        info[DD_INSTANCE_KEY] = vt_c_obj[VT_SCAL_KEY][VT_PROP_KEY][VT_COUNT_KEY]
    if VT_HOST_KEY in vt_c_obj and \
       VT_PROP_KEY in vt_c_obj[VT_HOST_KEY]:
        host_obj = vt_c_obj[VT_HOST_KEY][VT_PROP_KEY] 
        info[DD_VCPUS_KEY] = int(host_obj[VT_NUM_CPUS])
        info[DD_RAM_KEY] = import_size(host_obj[VT_MEM_SIZE_KEY])
        if VT_DISK_SIZE_KEY in host_obj:
            info[DD_DISK_KEY] = import_size(host_obj[VT_DISK_SIZE_KEY])
        if VT_NUM_GPUS_KEY in host_obj:
            info[DD_GPUS_KEY] = int(host_obj[VT_NUM_GPUS_KEY])
        if info[DD_INSTANCE_KEY] is None: 
            info[DD_INSTANCE_KEY] = 1
    if VT_OS_KEY in vt_c_obj and \
       VT_PROP_KEY in vt_c_obj[VT_OS_KEY]:
        os_obj = vt_c_obj[VT_OS_KEY][VT_PROP_KEY]
        info[DD_OS_DISTRO_KEY] = str(os_obj[VT_DISTRIBUTION_KEY])
        info[DD_OS_VERSION_KEY] = str(os_obj[VT_VERSION_KEY])
    return info

def get_storage_info(vt_s_obj: dict) -> dict:
    """vt_s_obj = Validated Template Storage Section Object"""
    info = dict()
    if VT_SIZE_KEY in vt_s_obj:
        info[DD_STORAGE_KEY] = import_size(vt_s_obj[VT_SIZE_KEY])
        info[DD_VOLUMES_KEY] = 1
    return info

# Import data in template
def extract_data_from_valid_template(val_templ: dict) -> dict:
    if "dep_name" in val_templ:
        val_templ['template_name'] = val_templ['dep_name']
        del val_templ['dep_name']
    dep_data = {k:val_templ[k] for k in VT_KEYS_TO_IMPORT}
    dep_data[DD_NODE_INFO_KEY] = dict()
    dep_data[DD_STORAGE_INFO_KEY] = dict()
    dep_data[DD_PROVIDERS_KEY] = dict()
    top_templ = val_templ[VT_TOPOLOGY_KEY]
    for node_key, node_obj in top_templ[VT_NODE_KEY].items():
        if node_obj[VT_TYPE_KEY] == VT_BLOCKSTORAGE_TYPE and \
           VT_PROP_KEY in node_obj:
            vt_s_obj = node_obj[VT_PROP_KEY]
            dep_data[DD_STORAGE_INFO_KEY][node_key] = get_storage_info(vt_s_obj)
        elif node_obj[VT_TYPE_KEY] == VT_COMPUTER_TYPE and \
             VT_CAPABILITIES_KEY in node_obj:
            vt_c_obj = node_obj[VT_CAPABILITIES_KEY]
            dep_data[DD_NODE_INFO_KEY][node_key] = get_compute_info(vt_c_obj)
    for node_key, node_obj in top_templ[VT_OUTPUTS_KEY].items():
        if VT_IP_SUFFIX in node_key:
            if VT_VALUE_KEY in node_obj and \
               VT_GET_ATTR_KEY in node_obj[VT_VALUE_KEY] and \
               len(node_obj[VT_VALUE_KEY][VT_GET_ATTR_KEY]) > 1 and \
               VT_PUB_ADDR_KEY in node_obj[VT_VALUE_KEY][VT_GET_ATTR_KEY][1]:
                node_name = node_obj[VT_VALUE_KEY][VT_GET_ATTR_KEY][0]
                if node_name in dep_data[DD_NODE_INFO_KEY]:
                    dep_data[DD_NODE_INFO_KEY][node_name][DD_FLOATINGIPS_KEY] = 1
    return dep_data

def get_info_from_provider(obj: dict) -> dict:
    info = {DD_IMAGES_KEY: False,
            DD_QUOTAS_KEY: dict(),
            DD_BEST_FLAVOR_KEY: dict(),
            DD_PROVIDER_NAME_KEY: obj[PD_PROVIDER_NAME_KEY],
            DD_REGION_NAME_KEY: obj[PD_REGION_NAME_KEY]
           }   
    return info

def create_key(base: str, usage: bool) -> str:
    return (base + DD_USAGE_SUFFIX) if usage else (base + DD_QUOTA_SUFFIX)

def collect_quotas(prov_data: dict) -> dict:
    quotas = dict()
    for prov_key,prov_obj in prov_data.items():
        if prov_key in M_QUOTAS_KEYS:
            for obj in prov_obj:
                for q_elem in obj[PD_QUOTAS_KEY]:
                    for dd_key, pd_key, factor in M_QUOTAS_KEYS[prov_key]:
                        obj_key = create_key(dd_key, q_elem[PD_USAGE_KEY])
                        obj_value = int(q_elem[pd_key]/factor)
                        quotas[obj_key] = obj_value
    return quotas

def is_param_inclused(dn_obj, pf_obj):
    return dn_obj <= pf_obj if dn_obj is not None and pf_obj is not None else True

def are_params_inclused(n_obj, f_obj):
    return all([is_param_inclused(n_obj[dn_key], f_obj[pf_key] / f)
               for dn_key, pf_key, f in MISC_BEST_MATCHER_KEYS])

def get_best_flavor(node_obj, prov_flavors):
    best_flavor = {k:sys.maxsize for k in DD_FLAVOR_KEYS}
    for prov_flavor in prov_flavors:
        if are_params_inclused(node_obj, prov_flavor):
            if all([prov_flavor[pf_key]/f <= best_flavor[dn_key] 
                    for dn_key, pf_key, f in MISC_BEST_MATCHER_KEYS]):
                best_flavor = {dn_key: prov_flavor[pf_key]/f 
                               for dn_key, pf_key, f in MISC_BEST_MATCHER_KEYS}
    temp = node_obj.copy()
    temp.update(best_flavor)
    return temp

#Find is all needed images for the deployments are available in the given cloud project
def check_single_image(node,image):
    return node[DD_OS_DISTRO_KEY]  == image[PD_OS_DISTRO_KEY] and \
           node[DD_OS_VERSION_KEY] == image[PD_OS_VERSION_KEY]

def is_image_present(node,data):
    images = data[PD_COMPUTE_SERVICES_KEY][0][PD_IMAGES_KEY]
    return any(check_single_image(node,image) for image in images)

def are_present_images(dep_data, prov_data):
    nodes = dep_data[DD_NODE_INFO_KEY].values() 
    return all(is_image_present(node,prov_data) for node in nodes
               if node[DD_INSTANCE_KEY] > 0)

def best_match_finder(dep_data, prov_data):
    for p_key, p_obj in prov_data.items():
        if p_obj[PD_USER_GROUP_KEY] != dep_data[DD_USER_GROUP_KEY]: 
            continue
   
        info = get_info_from_provider(p_obj)
        info[DD_QUOTAS_KEY] = collect_quotas(p_obj)
        info[DD_IMAGES_KEY] = are_present_images(dep_data, p_obj)
        
        for n_name, n_obj in dep_data[DD_NODE_INFO_KEY].items():
            if n_obj[DD_INSTANCE_KEY] < 1: continue
            p_flavors = p_obj[PD_COMPUTE_SERVICES_KEY][0][PD_FLAVORS_KEY]
            info[DD_BEST_FLAVOR_KEY][n_name] = get_best_flavor(n_obj, p_flavors)
            dep_data[DD_PROVIDERS_KEY][p_key] = info
    return dep_data

def remove_null(dep_data):
    for _, prov_data in dep_data[DD_PROVIDERS_KEY].items():
        for flavor_obj in prov_data[DD_BEST_FLAVOR_KEY].values():
            for flavor_key, flavor_value in flavor_obj.items():
                if flavor_value is None:
                    flavor_obj[flavor_key] = 0
    return dep_data

def get_rally_provider_name(prov_data):
    keys_to_use = [PD_PROVIDER_NAME_KEY, 
                   PD_REGION_NAME_KEY]
    str_key = '-'.join([prov_data[k] for k in keys_to_use])
    return RALLY_PROVIDER_NAME_MAPPING[str_key]

def compute_aggregated_resource(data):
    for dep_prov_obj in data[DD_PROVIDERS_KEY].values():
        aggr_res = dict()
        nodes = dep_prov_obj[DD_BEST_FLAVOR_KEY].values()
        for fl_key in sorted(DD_AGGR_KEYS):
            acc_value = sum(node[fl_key] * node[DD_INSTANCE_KEY] 
                            for node in nodes 
                            if node[DD_INSTANCE_KEY] > 0)
            aggr_res[fl_key + DD_REQUIRED_SUFFIX] = acc_value
        acc_value = sum([node[DD_INSTANCE_KEY] for node in nodes])
        aggr_res[DD_INSTANCE_KEY + DD_REQUIRED_SUFFIX] = acc_value
        dep_prov_obj[DD_AGGREGATED_RESOURCES_KEY] = aggr_res

        rally_prov_name = get_rally_provider_name(dep_prov_obj)
        rally_value, err = get_rally_perc_failures(rally_prov_name)
        dep_prov_obj[DD_RALLY_VALUE_KEY] = -1 if err else rally_value

    data[DD_AGGREGATED_STORAGE_KEY] = dict()
    for stor_key in DD_STORAGE_SEC_KEYS:
        dep_stor_key = stor_key + DD_REQUIRED_SUFFIX
        dep_storage_obj = data[DD_STORAGE_INFO_KEY].values()
        acc_value = sum([storage_value.get(stor_key,0) 
                         for storage_value in dep_storage_obj])
        data[DD_AGGREGATED_STORAGE_KEY][dep_stor_key] = acc_value
    return data

def get_msg(dep_data):
    m = {k:dep_data[k] for k in DD_KEYS_TO_IMPORT} 
	m[O_MSG_VERSION] = O_MSG_VERSION_VALUE
    m[O_PROVIDERS_KEY] = list()
    for provider in dep_data[DD_PROVIDERS_KEY].values():
        p_info = provider[DD_AGGREGATED_RESOURCES_KEY]
        del p_info[DD_DISK_KEY + DD_REQUIRED_SUFFIX]
        p_info[O_PROVIDER_NAME_KEY] = provider[DD_PROVIDER_NAME_KEY]
        p_info[O_REGION_NAME_KEY] = provider[DD_REGION_NAME_KEY]
        p_info[O_IMAGES_KEY] = provider[DD_IMAGES_KEY]
        p_info[O_TEST_FAILURE_PERC] = provider[DD_RALLY_VALUE_KEY]
        p_info.update(provider[DD_QUOTAS_KEY])
        p_info.update(dep_data[DD_AGGREGATED_STORAGE_KEY])
        for k,v in p_info.items():
            if isinstance(v,int): 
                p_info[k] = float(v)
        m[O_PROVIDERS_KEY].append(p_info)
    return m
