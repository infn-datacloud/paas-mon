import logging.handlers
import os 
import logging
logger = logging.getLogger(__name__)

def conf_logger(app_dir_base, app_name):
    log_filename = f"{app_dir_base}/{app_name}.log"
    logger = logging.getLogger(app_name)
    formatter_str = '%(asctime)s [%(name)-12s] %(levelname)s %(message)s'
    formatter = logging.Formatter(formatter_str)
    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)
    logger.addHandler(stream_handler)
    file_handler = logging.handlers.RotatingFileHandler(log_filename,
                                                        maxBytes=10*1024*1024,
                                                        backupCount=7)
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    logger.setLevel(logging.INFO)
    return logger

def conf_init_logger():
    logger = logging.getLogger("INIT")
    formatter_str = '%(asctime)s [%(name)-12s] %(levelname)-8s %(message)s'
    formatter = logging.Formatter(formatter_str)
    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)
    logger.addHandler(stream_handler)
    logger.setLevel(logging.INFO)
    return logger

class Defaults:
    APP_DIR_BASE = "/kafka-components/producer-rally"
    APP_NAME = "prod-rally"
    DATA_DIR = "/data"
    KAFKA_ACK = "all"
    KAFKA_ALLOW_AUTO_CREATE_TOPICS = False
    KAFKA_CLUSTER = 'kafka01:9095,kafka02:9095,kafka03:9095'
    KAFKA_CONSUMER_TIMEOUT_MS = 1000
    KAFKA_INPUT_TOPICS = 'rally'
    KAFLA_MAX_REQUEST_SIZE = 104857600
    KAFKA_SSL_PASSWORD_PATH = f'{APP_DIR_BASE}/certs/prod_rally.password'
    KAFKA_SSL_CACERT_PATH = f'{APP_DIR_BASE}/certs/ca_cert.pem'
    KAFKA_SSL_CERT_PATH = f'{APP_DIR_BASE}/certs/prod_rally_cert_signed.pem'
    KAFLA_SSL_KEY_PATH = f'{APP_DIR_BASE}/certs/prod_rally_key.pem'
    KAFKA_OUTPUT_TOPIC = 'rally'

class EnvVars:
    APP_DIR_BASE = "APP_DIR_BASE"
    APP_NAME = "APP_NAME"
    DATA_DIR = 'DATA_DIR'
    PROCESSOR_STR_FILTER = 'PROCESSOR_STR_FILTER' 
    KAFKA_ACK = "KAFKA_ACK"
    KAFKA_ALLOW_AUTO_CREATE_TOPICS = "KAFKA_ALLOW_AUTO_CREATE_TOPICS"
    KAFKA_CLUSTER = 'KAFKA_CLUSTER'
    KAFKA_CONSUMER_TIMEOUT_MS = 'KAFKA_CONSUMER_TIMEOUT_MS'
    KAFKA_INPUT_TOPICS = 'KAFKA_INPUT_TOPICS'
    KAFLA_MAX_REQUEST_SIZE = "KAFLA_MAX_REQUEST_SIZE"
    KAFKA_SSL_PASSWORD_PATH = 'KAFKA_SSL_PASSWORD_PATH'
    KAFKA_SSL_CACERT_PATH = 'KAFKA_SSL_CACERT_PATH'
    KAFKA_SSL_CERT_PATH = 'KAFKA_SSL_CERT_PATH'
    KAFLA_SSL_KEY_PATH = 'KAFLA_SSL_KEY_PATH'
    KAFKA_OUTPUT_TOPIC = 'KAFKA_OUTPUT_TOPIC'

class Configuration:
    def __init__(self):
        
        self.logger = conf_init_logger()
        self.logger.info("START Configuration collecting...")
        
        # Inizializza gli attributi con i valori caricati
        self.app_dir_base = self._load_param(
            env_var_name=EnvVars.APP_DIR_BASE,
            default_value=Defaults.APP_DIR_BASE,
            param_type=str
        )
        self.app_name = self._load_param(
            env_var_name=EnvVars.APP_NAME,
            default_value=Defaults.APP_NAME,
            param_type=str
        )
        
        self.data_dir = self._load_param(
            env_var_name=EnvVars.DATA_DIR,
            default_value=Defaults.DATA_DIR,
            param_type=str
        )
        
        self.logger = conf_logger(self.app_dir_base, 
                                  self.app_name)
        
        self.kafka_ack = self._load_param(
            env_var_name=EnvVars.KAFKA_ACK,
            default_value=Defaults.KAFKA_ACK,
            param_type=str
        )
        self.kafka_allow_auto_create_topics = self._load_param(
            env_var_name=EnvVars.KAFKA_ALLOW_AUTO_CREATE_TOPICS,
            default_value=Defaults.KAFKA_ALLOW_AUTO_CREATE_TOPICS,
            param_type=bool
        )
        self.kafka_cluster = self._load_param(
            env_var_name=EnvVars.KAFKA_CLUSTER,
            default_value=Defaults.KAFKA_CLUSTER,
            param_type=str
        )        
        
        self.kafka_consumer_timeout_ms = self._load_param(
            env_var_name=EnvVars.KAFKA_CONSUMER_TIMEOUT_MS,
            default_value=Defaults.KAFKA_CONSUMER_TIMEOUT_MS,
            param_type=int
        )
        
        self.kafka_input_topics = self._load_param(
            env_var_name=EnvVars.KAFKA_INPUT_TOPICS,
            default_value=Defaults.KAFKA_INPUT_TOPICS,
            param_type=int
        )
        
        self.kafka_max_request_size = self._load_param(
            env_var_name=EnvVars.KAFLA_MAX_REQUEST_SIZE,
            default_value=Defaults.KAFLA_MAX_REQUEST_SIZE,
            param_type=int
        )
        self.kafka_ssl_password_path = self._load_param(
            env_var_name=EnvVars.KAFKA_SSL_PASSWORD_PATH,
            default_value=Defaults.KAFKA_SSL_PASSWORD_PATH,
            param_type=str
        )
        self.kafka_ssl_cacert_path = self._load_param(
            env_var_name=EnvVars.KAFKA_SSL_CACERT_PATH,
            default_value=Defaults.KAFKA_SSL_CACERT_PATH,
            param_type=str
        )
        self.kafka_ssl_cert_path = self._load_param(
            env_var_name=EnvVars.KAFKA_SSL_CERT_PATH,
            default_value=Defaults.KAFKA_SSL_CERT_PATH,
            param_type=str
        )
        self.kafka_ssl_key_path = self._load_param(
            env_var_name=EnvVars.KAFLA_SSL_KEY_PATH,
            default_value=Defaults.KAFLA_SSL_KEY_PATH,
            param_type=str
        )
        self.kafka_output_topic = self._load_param(
            env_var_name=EnvVars.KAFKA_OUTPUT_TOPIC,
            default_value=Defaults.KAFKA_OUTPUT_TOPIC,
            param_type=str
        )
        self.logger.info("END Configuration collecting.")
        # self.logger.info(f"Loaded Parameters: {self.__dict__}")

    def _load_param(self, env_var_name: str, default_value, param_type):
        """
        Loading of a single parameter. 
        First find the environment variable then use the default value is the not present.
        Try value cast to the specified type.
        """
        env_value = os.getenv(env_var_name)

        if env_value is not None:
            try:
                if param_type == bool:
                    if env_value.lower() in ('true', '1', 'yes'):
                        converted_value = True
                    elif env_value.lower() in ('false', '0', 'no'):
                        converted_value = False
                    else:
                        raise ValueError(f"Boolean value not recognised: '{env_value}'")
                else:
                    converted_value = param_type(env_value)
                self.logger.debug(f"Loaded {env_var_name} from envvar: '{env_value}' -> {converted_value} (type: {param_type.__name__})")
                return converted_value
            except ValueError as e:
                self.logger.warning(
                    f"Env Var '{env_var_name}' with value '{env_value}' "
                    f"is not possible to convert to the type {param_type.__name__}. "
                    f"Default value will be used: {default_value}"
                    f"Error: {e}"
                )
                return default_value
        else:
            self.logger.debug(f"Env Var '{env_var_name}' not found. Used default value: '{default_value}'")
            return default_value

    def __repr__(self):
        new_line = '\n'
        tab = '\t'
        return f"{new_line.join(f'{tab}{k}={repr(v)}' for k, v in self.__dict__.items())}"
    
    def get_logger(self):
        return self.logger