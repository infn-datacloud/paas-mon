# FEEDER CONSTANTS (F prefix)
## Feeded message fields (aka keys)
F_KAFKA_TS_KEY = 'kafka_ts'
F_TIMESTAMP_KEY = 'timestamp'
F_PROVIDER_TO_REJECT = ["CLOUD-CNAF-TB"]

# RALLY CONSTANTS (R prefix)
## Rally dataset column names
R_PROVIDER_COL = 'provider'
R_STATUS_COL = 'status'
R_TEST_RESULT_COL = 'test_result'
R_TIMESTAMP_COL = 'timestamp'

## Rally column values
R_STATUS_FINISHED = 'finished'
R_TEST_RESULT_TRUE = 'True'
R_TEST = False

## Rally provider name mapping
## This hard coded mapping should be removed when the providers will use normalized names
## according to the naming convention defined in the documentation
## TODO: remove hard coded mapping``
RALLY_PROVIDER_NAME_MAPPING = {
    'BACKBONE-bari': 'backbone_bari',
    'BACKBONE-cnaf': 'backbone_cnaf',
    'CLOUD-CNAF-T1-tier1': 'cloud_cnaf_t1',
    'CLOUD-INFN-CATANIA-INFN-CT': 'cloud_ct' ,
    'RECAS-BARI-RegionOne': 'recas_bari',
    "CLOUD-VENETO-regionOne": 'cloud_veneto',
    "CLOUD_NAPOLIXXXXXX": 'cloud_na'
}

# VALIDATED TEMPLATE CONSTANTS  (VT prefix)
VT_BLOCKSTORAGE_INDIGO_TYPE = 'tosca.nodes.indigo.BlockStorage'
VT_BLOCKSTORAGE_TYPE = 'tosca.nodes.BlockStorage'
VT_CAPABILITIES_KEY = 'capabilities'
VT_COMPUTER_TYPE = 'tosca.nodes.indigo.Compute'
VT_COUNT_KEY = 'count'
VT_DISK_SIZE_KEY = 'disk_size'
VT_DISTRIBUTION_KEY = 'distribution'
VT_GET_ATTR_KEY = 'get_attribute'
VT_HOST_KEY = 'host'
VT_IP_SUFFIX = '_ip'
VT_MEM_SIZE_KEY =  'mem_size'
VT_NODE_KEY = 'node_templates'
VT_NUM_CPUS = 'num_cpus'
VT_NUM_GPUS_KEY = 'num_gpus'
VT_OS_KEY = 'os'
VT_OUTPUTS_KEY = 'outputs'
VT_PROP_KEY = 'properties'
VT_PUB_ADDR_KEY = 'public_address'
VT_SCAL_KEY = 'scalable'
VT_SIZE_KEY = 'size'
VT_TEMPLATE_NAME_KEY = 'template_name'
VT_TIMESTAMP_KEY = 'timestamp'
VT_TOPOLOGY_KEY = 'topology_template'
VT_TYPE_KEY = 'type'
VT_USER_GROUP_KEY = 'user_group'
VT_UUID_KEY = 'uuid'
VT_VALUE_KEY = 'value'
VT_VERSION_KEY = 'version'

## Size suffixes 
VT_GB_SIZE_SUFFIX = ' GB'
VT_PB_SIZE_SUFFIX = ' PB'
VT_TB_SIZE_SUFFIX = ' TB'

## Constant collection
VT_KEYS_TO_IMPORT = (VT_TEMPLATE_NAME_KEY, 
                     VT_TIMESTAMP_KEY, 
                     VT_USER_GROUP_KEY, 
                     VT_UUID_KEY)

# FEEDER PROVIDER DATA CONSTANTS (PD prefix)
PD_BLOCK_STORAGE_SERVICES_KEY = "block_storage_services"
PD_COMPUTE_SERVICES_KEY = "compute_services"
PD_CORES_KEY = 'cores'
PD_DISK_KEY = 'disk'
PD_FLAVORS_KEY = 'flavors'
PD_GIGABYTES_KEYS = 'gigabytes'
PD_GPUS_KEY = 'gpus'
PD_IMAGES_KEY = 'images'
PD_INSTANCES_KEY = 'instances'
PD_MSG_VERSION = 'msg_version'
PD_NETWORK_SERVICES_KEY = "network_services"
PD_OS_DISTRO_KEY = 'os_distro'
PD_OS_VERSION_KEY = 'os_version'
PD_PROJECT_ID_KEY = 'project_id'
PD_PROVIDER_NAME_KEY = 'provider_name'
PD_PUBLIC_IPS_KEY = 'public_ips'
PD_QUOTAS_KEY = 'quotas'
PD_RAM_KEY = 'ram'
PD_REGION_NAME_KEY = 'region_name'
PD_TEMPL_VOLUMES = 'volumes'
PD_USAGE_KEY = 'usage'
PD_USER_GROUP_KEY = 'user_group'
PD_VCPUS_KEY = 'vcpus'
PD_TEST_TIMESTAMP = "kafka_ts"
PD_OVERBOOKING_CPU = "overbooking_cpu"
PD_OVERBOOKING_RAM = "overbooking_ram"
PD_BANDWITDH_IN = "bandwidth_in"
PD_BANDWITDH_OUT = "bandwidth_out"

## Constant Collections
PD_PROVIDER_ID_KEYS = (PD_PROVIDER_NAME_KEY, 
                       PD_REGION_NAME_KEY, 
                       PD_PROJECT_ID_KEY)

# DEPLOYMENT AGGREGATED DATA CONSTANTS (DD prefix)
DD_NODE_INFO_KEY = 'requested-nodes'
DD_STORAGE_INFO_KEY = 'storage'
DD_FLOATINGIPS_KEY = 'floating_ips'
DD_PROVIDERS_KEY = 'providers'
DD_INSTANCE_KEY = 'n_instances'
DD_VCPUS_KEY = 'vcpus'
DD_RAM_KEY = 'ram_gb'
DD_OS_DISTRO_KEY = 'os_distro'
DD_OS_VERSION_KEY = 'os_version'
DD_DISK_KEY = 'disk_gb'
DD_GPUS_KEY = 'gpus'
DD_STORAGE_KEY = 'storage_gb'
DD_USER_GROUP_KEY = 'user_group'
DD_VOLUMES_KEY = 'n_volumes'
DD_BEST_FLAVOR_KEY = 'best_flavor'
DD_IMAGES_KEY = 'images'
DD_QUOTAS_KEY = 'quotas'
DD_PROVIDER_NAME_KEY = 'provider_name'
DD_REGION_NAME_KEY = 'region_name'
DD_REQUIRED_SUFFIX = '_requ'
DD_QUOTA_SUFFIX = '_quota'
DD_USAGE_SUFFIX = '_usage'
DD_AGGREGATED_RESOURCES_KEY = 'aggr_resource'
DD_AGGREGATED_STORAGE_KEY = 'aggregated_storage'
DD_RALLY_N_DAYS = [30, 7, 1]
DD_RALLY_DAY = 'd'
DD_RALLY_VALUE_KEY = 'rally_value'
DD_TEMPLATE_NAME_KEY = 'template_name'
DD_TIMESTAMP_KEY = 'timestamp'
DD_UUID_KEY = 'uuid'
DD_EXACT_FLAVOR = "exact_flavor"
DD_EXACT_FLAVORS = "exact_flavors"
DD_FOUND_FLAVOR = "found_flavor"
DD_OVERBOOKING_CPU = "overbooking_cpu"
DD_OVERBOOKING_RAM = "overbooking_ram"
DD_BANDWITDH_IN = "bandwidth_in"
DD_BANDWITDH_OUT = "bandwidth_out"
DD_COMP_KEYS = (DD_INSTANCE_KEY, 
                DD_VCPUS_KEY, 
                DD_RAM_KEY, 
                DD_DISK_KEY, 
                DD_OS_DISTRO_KEY, 
                DD_OS_VERSION_KEY, 
                DD_FLOATINGIPS_KEY, 
                DD_GPUS_KEY)

DD_FLAVOR_KEYS = (DD_DISK_KEY,
                  DD_GPUS_KEY,
                  DD_RAM_KEY, 
                  DD_VCPUS_KEY)

DD_AGGR_KEYS = (*DD_FLAVOR_KEYS,
                DD_FLOATINGIPS_KEY,
                DD_EXACT_FLAVOR)

DD_STORAGE_SEC_KEYS = (DD_STORAGE_KEY, 
                       DD_VOLUMES_KEY)

# DD_KEYS_TO_IMPORT = [DD_TEMPLATE_NAME_KEY, 
#                      DD_TIMESTAMP_KEY, 
#                      DD_USER_GROUP_KEY, 
#                      DD_UUID_KEY]

# OUTPUT MESSAGE CONSTANTS (O prefix)
O_PROVIDERS_KEY = 'providers'
O_PROVIDER_NAME_KEY = 'provider_name'
O_REGION_NAME_KEY = 'region_name'
O_IMAGES_KEY = 'images'
O_TEST_FAILURE_PERC = 'test_failure_perc'
O_TIMESTAMP = 'submission_time'
O_TEMPLATE_NAME = 'template_name'
O_USER_GROUP_KEY = 'user_group'
O_UUID_KEY = 'uuid'
O_OVERBOOKING_CPU = "overbooking_cpu"
O_OVERBOOKING_RAM = "overbooking_ram"
O_BANDWITDH_IN = "bandwidth_in"
O_BANDWITDH_OUT = "bandwidth_out"
O_MSG_VERSION = 'msg_version'
O_MSG_VERSION_VALUE = '1.1.0'

# MISCELLANEOUS
O_DD_KEYS_TO_IMPORT = [(O_TEMPLATE_NAME, DD_TEMPLATE_NAME_KEY), 
                       (O_TIMESTAMP, DD_TIMESTAMP_KEY),  
                       (O_USER_GROUP_KEY,DD_USER_GROUP_KEY),
                       (O_UUID_KEY, DD_UUID_KEY)]

M_QUOTAS_KEYS = {
    PD_NETWORK_SERVICES_KEY:       [(DD_FLOATINGIPS_KEY, PD_PUBLIC_IPS_KEY,     1)],
    PD_COMPUTE_SERVICES_KEY:       [(DD_VCPUS_KEY,       PD_CORES_KEY,          1),
                                           (DD_RAM_KEY,         PD_RAM_KEY,        1_024),
                                           (DD_INSTANCE_KEY,    PD_INSTANCES_KEY,      1)],
    PD_BLOCK_STORAGE_SERVICES_KEY: [(DD_STORAGE_KEY,     PD_GIGABYTES_KEYS,     1),
                                           (DD_VOLUMES_KEY,     PD_TEMPL_VOLUMES,      1)]
}

MISC_BEST_MATCHER_KEYS = ((DD_DISK_KEY,  PD_DISK_KEY,      1),
                          (DD_GPUS_KEY,  PD_GPUS_KEY,      1),
                          (DD_RAM_KEY,   PD_RAM_KEY,   1_024),
                          (DD_VCPUS_KEY, PD_VCPUS_KEY,     1))

BASIC_PROV_DATA_INFO = [
    (DD_PROVIDER_NAME_KEY, PD_PROVIDER_NAME_KEY),
    (DD_REGION_NAME_KEY,   PD_REGION_NAME_KEY),
    (DD_OVERBOOKING_CPU, PD_OVERBOOKING_CPU),
    (DD_OVERBOOKING_RAM, PD_OVERBOOKING_RAM),
    (DD_BANDWITDH_IN, PD_BANDWITDH_IN),
    (DD_BANDWITDH_OUT, PD_BANDWITDH_OUT)
]

O_PROV_DATA = [
    (O_PROVIDER_NAME_KEY, DD_PROVIDER_NAME_KEY),
    (O_REGION_NAME_KEY,   DD_REGION_NAME_KEY),
    (O_OVERBOOKING_CPU, DD_OVERBOOKING_CPU),
    (O_OVERBOOKING_RAM, DD_OVERBOOKING_RAM),
    (O_BANDWITDH_IN, DD_BANDWITDH_IN),
    (O_BANDWITDH_OUT, DD_BANDWITDH_OUT)
]

# Environment variables
## Kafka parameteres
KAFKA_VAL_TEMPL_TOPIC =   'INFER_PROC_KAFKA_VAL_TEMPL_TOPIC'
KAFKA_RALLY_TOPIC =       'INFER_PROC_KAFKA_RALLY_TOPIC'
KAFKA_FEEDER_TOPIC =      'INFER_PROC_KAFKA_FEEDER_TOPIC'
KAFKA_INFER_TOPIC =       'INFER_PROC_KAFKA_INFER_TOPIC'
KAFKA_LOG_TOPIC =         'INFER_PROC_KAFKA_LOG_TOPIC'
KAFKA_BOOTSTRAP_SERVERS = 'INFER_PROC_KAFKA_BOOTSTRAP_SERVERS'

KAFKA_VAL_TEMPL_TOPIC_DEFAULT =   'validated-templates'
KAFKA_RALLY_TOPIC_DEFAULT =       'rally'
KAFKA_FEEDER_TOPIC_DEFAULT =      'federation-registry-feeder'
KAFKA_INFER_TOPIC_DEFAULT =       'ai-ranker-inference'
KAFKA_LOG_TOPIC_DEFAULT =         'logs-inference-processor'
KAFKA_BOOTSTRAP_SERVERS_DEFAULT = '192.168.21.96:9092'