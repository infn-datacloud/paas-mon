ORCLOG_SUBMISSION_LINE    = "Submission of deployment request to the IM. "
ORCLOG_COMPLETED_LINE     = "Deployment completed successfully. "
ORCLOG_ERROR_LINE         = "Deployment in error. "
ORCLOG_ORCHESTARTOR_TAG   = "orchestrator orchestrator/"
ORCLOG_ERROR_SUMMARY_LINE = " Retries on cloud providers exhausted."
ORCLOG_SYSLOG_TS_FORMAT   = "%Y-%m-%dT%H:%M:%S%z" # YYYY-MM-DD HH:MM:SS+ZZ:ZZ

ORCLOG_PROVIDER_NAME      = "provider_name"
ORCLOG_PROVIDER_REGION    = 'provider_region'
ORCLOG_UUID               = 'uuid'
ORCLOG_STATUS             = 'status'
ORCLOG_STATUS_REASON      = 'status_reason'


# Internal variables
INT_TIMESTAMP             = 'timestamp'

INT_CREATION_DATE = 'creation_date' # submission date
INT_STATUS = 'status'
INT_STATUS_REASON = 'status_reason'
INT_N_FAILURES = 'n_failures'
INT_TOT_FAILURE_TIME = 'tot_failure_time_s'
INT_COMPL_TIME = 'completetion_time_s'
INT_LAST_SUBMITTION_DATE = 'last_sub_date'
INT_PROVIDER_ID = 'provider_id'
INT_UUID = 'uuid'
INT_PROV_ID = [
    ORCLOG_PROVIDER_NAME,
    ORCLOG_PROVIDER_REGION
]

STATUS_SUBMITTED = 'SUBMISSION_EVENT'
STATUS_COMPLETED = 'SUCCESSFUL_EVENT'
STATUS_FAILED    =      'ERROR_EVENT'


ARI_PROVIDERS   = 'providers'
ARI_PROV_NAME   = 'provider_name'
ARI_REG_NAME    = 'region_name'
ARI_MSG_VERSION = 'msg_version'
ARI_TEMPL_NAME  = 'template_name'
ARI_USER_GROUP  = 'user_group'
ARI_UUID        = 'uuid'
ARI_FIELD_TO_COPY = [
    ARI_MSG_VERSION,
    ARI_TEMPL_NAME,
    ARI_USER_GROUP,
    ARI_UUID
]

ARI_FIELD_TO_KEY = [
    ARI_UUID,
    ARI_PROV_NAME,
    ARI_REG_NAME
]

ART_COMPL_TIME = 'completetion_time_s'
ART_CREATION_DATE = 'creation_date'
ART_N_FAILURE = 'n_failure'
ART_STATUS = 'status'
ART_STATUS_REASON = 'status_reason'
ART_TOT_FAILURE_TIME = 'tot_failure_time_s'

ART_FIELDS_TO_COPY = [
    (ART_COMPL_TIME, INT_COMPL_TIME),
    (ART_CREATION_DATE, INT_CREATION_DATE),
    (ART_N_FAILURE, INT_N_FAILURES),
    (ART_STATUS, INT_STATUS),
    (ART_STATUS_REASON, INT_STATUS_REASON),
    (ART_TOT_FAILURE_TIME, INT_TOT_FAILURE_TIME),
]


LINES_TO_REJECT = [
    'it.reply.orchestrator.exception.service.DeploymentException: Error executing request to IM'
]

# Environment variables
## Kafka parameteres
KAFKA_ORC_LOG_TOPIC =     'TRAIN_PROC_KAFKA_ORC_LOG_TOPIC'
KAFKA_AI_INFER_TOPIC =    'TRAIN_PROC_KAFKA_AI_INFER_TOPIC'
KAFKA_AI_TRAIN_TOPIC =    'TRAIN_PROC_KAFKA_AI_TRAIN_TOPIC'
KAFKA_LOG_TOPIC =         'TRAIN_PROC_KAFKA_LOG_TOPIC'
KAFKA_BOOTSTRAP_SERVERS = 'TRAIN_PROC_KAFKA_BOOTSTRAP_SERVERS'

KAFKA_ORC_LOG_TOPIC_DEFAULT =     'orchestrator-logs'
KAFKA_AI_INFER_TOPIC_DEFAULT =    'ai-ranker-inference'
KAFKA_AI_TRAIN_TOPIC_DEFAULT =    'ai-ranker-training'
KAFKA_LOG_TOPIC_DEFAULT =         'logs-training-processor'
KAFKA_BOOTSTRAP_SERVERS_DEFAULT = '192.168.21.96:9092'


LOG_SUBMISSION_EVENT = "Detected Submission event for uuid: "
LOG_SUCCESSFUL_EVENT = "Detected Successful event for uuid: "
LOG_ERROR_EVENT = "Detected Error event for uuid: "

LOG_STATUS_NOT_UUID_FOUND = "OK_NOT_UUID_FOUND"
LOG_STATUS_OK_SENT = "OK_SENT"
LOG_STATUS_OK_NOT_SENT = "OK_NOT_SENT"
LOG_STATUS_COLLECTED_AND_SENT = "Status Collected and Sent"
LOG_STATUS_COLLECTED = "Status Collected"