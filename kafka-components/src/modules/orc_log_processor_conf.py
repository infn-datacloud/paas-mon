# App parameters
USE_CONSTRAINTS = True

ORC_LOG_SEP = "]: "
ORC_LOG_EVENT_TEMPLATE_SEP = "i.r.o.service.DeploymentServiceImpl      : " 
ORC_LOG_START_TEMPLATE_STRING = "Creating deployment with template"
ORC_LOG_INFO_TEMPLATE_STRING = "{\"uuid\"" 
SYSLOG_TS_FORMAT = "%Y-%m-%dT%H:%M:%S%z"  # YYYY-MM-DD HH:MM:SS+ZZ:ZZ
ORC_TS_FORMAT = "%Y-%m-%d %H:%M:%S.%f"  # YYYY-MM-DD HH:MM:SS
ORC_LOG_FILTER = "orchestrator orchestrator/"

INPUT_USER_PARAMS = 'user_parameters'

MSG_PARAM_ERROR1 = "User did not provide the parameter, not default provided and parameter not required."
MSG_PARAM_ERROR2 = "User did not provide the parameter and parameter required."
MSG_PARAM_ERROR3 = "No constraint collected"
MSG_NO_ERR = ""

PARAM_DEFAULT = 'default'
PARAM_REQUIRED = 'required'
PARAM_REQUIRED_TRUE = True
PARAM_REQUIRED_FALSE = False
PARAM_CONSTRAINTS = 'constraints'
PARAM_VALID_VALUES = 'valid_values'
PARAM_N_CPUS = 'num_cpus'
PARAM_MEM = 'mem_size'
PARAM_PORTS = 'ports'
PARAM_SERVICE_PORTS = 'service_ports'
#PARAM_TO_NOT_CHECK = [PARAM_N_CPUS,PARAM_MEM,PARAM_PORTS,PARAM_SERVICE_PORTS]
PARAM_TO_NOT_CHECK = [PARAM_N_CPUS,PARAM_MEM,PARAM_PORTS]

OUTPUT_LOG_TEMPL_VALID_AND_SENT = "template validated and sent"
OUTPUT_LOG_TEMPL_VALID_AND_NOT_SENT = "template validated and already written"
OUTPUT_LOG_OK_STATUS = 'ok'
OUTPUT_LOG_NOK_STATUS = 'nok'

TEMPL_GET_INPUT = 'get_input'
TEMPL_NODE_TEMPL = 'node_templates'
TEMPL_DISPLAY_NAME = 'display_name'
TEMPL_DESCRIPTION = 'description'
TEMPL_DESCRIPTION_K8S = "kubernetes"
TEMPL_DESCRIPTION_K8S_VALUE = "Cluster Kubernetes"
TEMLP_INPUTS = 'inputs'
TEMPL_MSG_VERSION = 'msg_version'
TEMPL_MSG_VERSION_VALUE = "1.0.0"
TEMPL_TEMPL_NAME = 'template_name'
TEMPL_METADATA = 'metadata'
TEMPL_TOPOLOGY_TEMPL = 'topology_template'
TEMPL_USER_GROUP = 'user_group'
TEMPL_UUID = 'uuid'
TEMPL_TIMESTAMP = 'timestamp'

TYPE_FIELD = 'type'
TYPE_INTEGER = 'integer'
TYPE_SCALAR_UNIT_SIZE = "scalar-unit.size"
TYPE_STRING = 'string'
TYPE_VERSION = 'version'

TYPES_TO_INTEGER = [TYPE_INTEGER]
TYPES_TO_STRING = [TYPE_SCALAR_UNIT_SIZE,
                   TYPE_STRING,
                   TYPE_VERSION]


# Kafka parameteres
KAFKA_LOG_ORCHESTRATOR_TOPIC = 'TEMPLATE_PARSER_KAFKA_LOG_ORCHESTRATOR_TOPIC'
KAFKA_VAL_TEMPL_TOPIC =        'TEMPLATE_PARSER_KAFKA_VAL_TEMPL_TOPIC'
KAFKA_LOG_TOPIC =              'TEMPLATE_PARSER_KAFKA_LOG_APP_TOPIC'
KAFKA_BOOTSTRAP_SERVERS =      'TEMPLATE_PARSER_KAFKA_BOOTSTRAP_SERVERS'

KAFKA_LOG_ORCHESTRATOR_TOPIC_DEFAULT = 'orchestrator-logs'
KAFKA_VAL_TEMPL_TOPIC_DEFAULT =        'validated-templates'
KAFKA_LOG_TOPIC_DEFAULT =              'logs-parser-templates'
KAFKA_BOOTSTRAP_SERVERS_DEFAULT =      '192.168.21.96:9092'

# Log parameters
LOG_STATUS_OK = 'ok'
LOG_STATUS_NOK = 'nok'
LOG_MSG_VALIDATED = "valudated"
LOG_MSG_VALIDATED_AND_SENT = 'validated and template already present'
