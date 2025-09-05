import logging
import logging.handlers
import os
from modules.templateparser.settings import TemplateParserConfig

def create_logger(settings: TemplateParserConfig) -> logging.Logger:
    
    log_dir = settings.LOG_DIR
    app_name = settings.APP_NAME
    
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)
            
    log_filename = f"{log_dir}/{app_name}.log"
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
    logger.setLevel(logging.DEBUG)
    logger.info("Logger Configured")
    return logger