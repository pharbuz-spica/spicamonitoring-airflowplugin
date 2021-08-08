import requests
from ruxit.api.base_plugin import BasePlugin


class ApacheSparkDTPlugin(BasePlugin):
    def query(self, **kwargs):
        self.logger.info("Execution of plugin")
        self.logger.info("Activation context: %s", self.get_activation_context())
        base_url = 'http://localhost:4040/api/v1'
        app_list = self.get_apps(base_url)
        entities = self.get_monitored_entities()
        for entity in entities:
            self.logger.info("Monitored entity: id=%s, type=%s, group name=%s", entity.id, entity.type, entity.process_name)
            #if entity.process_name == 'Apache Spark apachespark':
            for app in app_list:
                app_name = app['name']
                job_list = self.get_jobs(base_url, app['id'])
                for job in job_list:
                    job_name = job['name']
                    entity.absolute(key='status_num', value=[self.get_job_status(job['status'])], dimensions=[job_name, app_name])
                    entity.absolute(key='numTasks', value=[job['numTasks']], dimensions=[job_name, app_name])
                    entity.absolute(key='numActiveTasks', value=[job['numActiveTasks']],
                                    dimensions=[job_name, app_name])
                    entity.absolute(key='numCompletedTasks', value=[job['numCompletedTasks']],
                                    dimensions=[job_name, app_name])
                    entity.absolute(key='numSkippedTasks', value=[job['numSkippedTasks']],
                                    dimensions=[job_name, app_name])
                    entity.absolute(key='numFailedTasks', value=[job['numFailedTasks']],
                                    dimensions=[job_name, app_name])
                    entity.absolute(key='numActiveStages', value=[job['numActiveStages']],
                                    dimensions=[job_name, app_name])
                    entity.absolute(key='numCompletedStages', value=[job['numCompletedStages']],
                                    dimensions=[job_name, app_name])
                    entity.absolute(key='numSkippedStages', value=[job['numSkippedStages']],
                                    dimensions=[job_name, app_name])
                    entity.absolute(key='numFailedStages', value=[job['numFailedStages']],
                                    dimensions=[job_name, app_name])
                executors_list = self.get_executors(base_url, app['id'])
                for executor in executors_list
                    exec_id = executor['id']
                    entity.absolute(key='rddBlocks', value=[job['rddBlocks']], dimensions=[app_name, exec_id])
                    entity.absolute(key='memoryUsed', value=[job['memoryUsed']], dimensions=[app_name, exec_id])
                    entity.absolute(key='diskUsed', value=[job['diskUsed']], dimensions=[app_name, exec_id])
                    entity.absolute(key='totalCores', value=[job['totalCores']], dimensions=[app_name, exec_id])
                    entity.absolute(key='maxTasks', value=[job['maxTasks']], dimensions=[app_name, exec_id])
                    entity.absolute(key='activeTasks', value=[job['activeTasks']], dimensions=[app_name, exec_id])
                    entity.absolute(key='failedTasks', value=[job['failedTasks']], dimensions=[app_name, exec_id])
                    entity.absolute(key='completedTasks', value=[job['completedTasks']], dimensions=[app_name, exec_id])
                    entity.absolute(key='totalTasks', value=[job['totalTasks']], dimensions=[app_name, exec_id])
                    entity.absolute(key='totalDuration', value=[job['totalDuration']], dimensions=[app_name, exec_id])
                    entity.absolute(key='totalGCTime', value=[job['totalGCTime']], dimensions=[app_name, exec_id])
                    entity.absolute(key='totalInputBytes', value=[job['totalInputBytes']], dimensions=[app_name, exec_id])
                    entity.absolute(key='totalShuffleRead', value=[job['totalShuffleRead']], dimensions=[app_name, exec_id])
                    entity.absolute(key='totalShuffleWrite', value=[job['totalShuffleWrite']], dimensions=[app_name, exec_id])
                    entity.absolute(key='maxMemory', value=[job['maxMemory']], dimensions=[app_name, exec_id])
                    entity.absolute(key='usedOnHeapStorageMemory',
                                    value=[job['memoryMetrics']['usedOnHeapStorageMemory']],
                                    dimensions=[app_name, exec_id])
                    entity.absolute(key='usedOffHeapStorageMemory',
                                    value=[job['memoryMetrics']['usedOffHeapStorageMemory']],
                                    dimensions=[app_name, exec_id])
                    entity.absolute(key='totalOnHeapStorageMemory',
                                    value=[job['memoryMetrics']['totalOnHeapStorageMemory']],
                                    dimensions=[app_name, exec_id])
                    entity.absolute(key='totalOffHeapStorageMemory',
                                    value=[job['memoryMetrics']['totalOffHeapStorageMemory']],
                                    dimensions=[app_name, exec_id])



    def get_apps(base_url):
        """Get the app ID from the REST server"""
        response = requests.get(base_url+'/applications')
        return response.json()

    def get_jobs(base_url, app_id):
        response = requests.get(base_url+'/applications/'+app_id+'/jobs')
        return response.json()

    def get_executors(base_url, app_id):
        response = requests.get(base_url+'/applications/'+app_id+'/executors')
        return response.json()

    def get_job_status(status):
        return_value = 0
        if status == 'RUNNING':
            return_value = 1
        elif status == 'SUCCEEDED':
            return_value = 2
        elif status == 'FAILED':
            return_value = -1
        return return_value

