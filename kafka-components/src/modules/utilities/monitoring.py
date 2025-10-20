import threading
from time import sleep

class Monitoring:

    def __init__(self, settings, logger, obj_to_monitor):
        self.settings = settings
        self.logger = logger
        
        if self.settings.MONITORING_ENABLED:
            self.obj_to_monitor = obj_to_monitor
            monitor_thread = threading.Thread(target=self.monitoring_task)
            monitor_thread.daemon = True
            monitor_thread.start()
        
    def monitoring_task(self):
        while True:
            mon = self.obj_to_monitor.get_mon_data()
            self.logger.info(f"Monitoring data: {mon}")
            sleep(self.settings.MONITORING_PERIOD)