from datetime import datetime, timedelta
import requests
import json
import asyncio
import logging
from ruxit.api.base_plugin import BasePlugin
from ruxit.api.selectors import ListenPortSelector, EntityType
from ruxit.api.data import PluginMeasurement


class ApacheSparkDTPlugin(BasePlugin):
    def query(self, **kwargs):
        self.logger.info("SPICA_SPARK_PLUGIN :::: Execution of plugin")
        self.logger.info("SPICA_SPARK_PLUGIN :::: Activation context: %s", self.get_activation_context())
        base_url_ws = self.config["base_url"] + '/ws/v1/cluster/apps'
        base_url_spark = self.config["base_url"] + '/proxy/'
        entity_selector = ListenPortSelector(self.config["port"], None, EntityType.PROCESS_GROUP_INSTANCE)
        #loop = asyncio.new_event_loop()
        #asyncio.set_event_loop(loop)

        try:
            app_list = self.get_apps(base_url_ws)
            self.logger.info("SPICA_SPARK_PLUGIN :::: Number of applications: %i" % (len(app_list['apps']['app'])))

            for app_ws in app_list['apps']['app']:

                #app_req = requests.get(base_url_spark + app_ws['id'] + '/api/v1/applications/' + app_ws['id'])

                #if app_req.status_code == 200:
                app_name = '{0} - ({1})'.format(app_ws['id'], app_ws['name'])
                app_id = app_ws['id']

                self.define_progress_metric(app_name, app_ws['progress'], entity_selector)

                job_list = self.get_jobs(base_url_spark, app_id)
                self.logger.info(
                    "SPICA_SPARK_PLUGIN :::: Number of jobs for app_id %s : %i" % (app_id, len(job_list)))
                self.define_job_metrics(job_list, entity_selector, app_name)
                executors_list = self.get_executors(base_url_spark, app_id)
                self.logger.info("SPICA_SPARK_PLUGIN :::: Number of executors for app_id %s : %i" % (
                    app_id, len(executors_list)))
                self.define_executor_metrics(executors_list, entity_selector, app_name)
                #else:
                #    self.logger.info("SPICA_SPARK_PLUGIN :::: 404, app not found")
            # loop.close()



        except requests.exceptions.RequestException as e:
            self.logger.info("SPICA_SPARK_PLUGIN :::: failed to connect, no jobs to monitor: {}".format(str(e)))

    def get_apps(self, base_url):
        """Get the app ID from the REST server"""
        params = {}
        params['state'] = 'RUNNING'

        response = requests.get(base_url, params=params)
        if response.status_code == 200:
            return response.json()
        else:
            self.logger.info(
                "SPICA_SPARK_PLUGIN :::: Using URL: %s, status code: %i" % (response.url, response.status_code))
            return json.loads("[]")

    def get_jobs(self, base_url, app_id):
        url = base_url + app_id + '/api/v1/applications/' + app_id + '/jobs'
        self.logger.info(
            "SPICA_SPARK_PLUGIN :::: Using URL: %s" % (url))
        response = requests.get(url)
        if response.status_code == 200:
            return response.json()
        else:
            self.logger.info("Using URL: %s, status code: %i" % (response.url, response.status_code))
            return json.loads("[]")

    def get_executors(self, base_url, app_id):
        url = base_url + app_id + '/api/v1/applications/' + app_id + '/executors'
        self.logger.info(
            "SPICA_SPARK_PLUGIN :::: Using URL: %s" % (url))
        response = requests.get(url)
        if response.status_code == 200:
            return response.json()
        else:
            self.logger.info("Using URL: %s, status code: %i" % (response.url, response.status_code))
            return json.loads("[]")

    def define_progress_metric(self, app_name, progress, entity_selector):
        self.results_builder.add_absolute_result(PluginMeasurement(key='progress', value=progress,
                                                                   dimensions={"app_name": app_name},
                                                                   entity_selector=entity_selector))

    def define_job_metrics(self, job_list, entity_selector, app_name):
        for job in job_list:
            job_name = job['name']
            self.logger.info("SPICA_SPARK_PLUGIN :::: Define metrics for job_name: %s" % (job_name))

            if 'numTasks' in job and job['numTasks'] > 0 :
                self.results_builder.add_absolute_result(PluginMeasurement(key='numTasks', value=job['numTasks'],
                                                                           dimensions={"job_name": job_name,
                                                                                       "app_name": app_name},
                                                                           entity_selector=entity_selector))
            if 'numActiveTasks' in job and job['numActiveTasks'] > 0:
                self.results_builder.add_absolute_result(
                    PluginMeasurement(key='numActiveTasks', value=job['numActiveTasks'],
                                      dimensions={"job_name": job_name, "app_name": app_name},
                                      entity_selector=entity_selector))
            if 'numCompletedTasks' in job and job['numCompletedTasks'] > 0:
                self.results_builder.add_absolute_result(
                    PluginMeasurement(key='numCompletedTasks', value=job['numCompletedTasks'],
                                      dimensions={"job_name": job_name, "app_name": app_name},
                                      entity_selector=entity_selector))
            if 'numSkippedTasks' in job and job['numSkippedTasks'] > 0:
                self.results_builder.add_absolute_result(
                    PluginMeasurement(key='numSkippedTasks', value=job['numSkippedTasks'],
                                      dimensions={"job_name": job_name, "app_name": app_name},
                                      entity_selector=entity_selector))
            if 'numFailedTasks' in job and job['numFailedTasks'] > 0:
                self.results_builder.add_absolute_result(
                    PluginMeasurement(key='numFailedTasks', value=job['numFailedTasks'],
                                      dimensions={"job_name": job_name, "app_name": app_name},
                                      entity_selector=entity_selector))
            if 'numActiveStages' in job and job['numActiveStages'] > 0:
                self.results_builder.add_absolute_result(
                    PluginMeasurement(key='numActiveStages', value=job['numActiveStages'],
                                      dimensions={"job_name": job_name, "app_name": app_name},
                                      entity_selector=entity_selector))
            if 'numCompletedStages' in job and job['numCompletedStages'] > 0:
                self.results_builder.add_absolute_result(
                    PluginMeasurement(key='numCompletedStages', value=job['numCompletedStages'],
                                      dimensions={"job_name": job_name, "app_name": app_name},
                                      entity_selector=entity_selector))
            if 'numSkippedStages' in job and job['numSkippedStages'] > 0:
                self.results_builder.add_absolute_result(
                    PluginMeasurement(key='numSkippedStages', value=job['numSkippedStages'],
                                      dimensions={"job_name": job_name, "app_name": app_name},
                                      entity_selector=entity_selector))
            if 'numFailedStages' in job and job['numFailedStages'] > 0:
                self.results_builder.add_absolute_result(
                    PluginMeasurement(key='numFailedStages', value=job['numFailedStages'],
                                      dimensions={"job_name": job_name, "app_name": app_name},
                                      entity_selector=entity_selector))

    def define_executor_metrics(self, executors_list, entity_selector, app_name):
        for executor in executors_list:
            exec_id = executor['id']
            host = executor['hostPort'].split(':')[0]
            self.logger.info("SPICA_SPARK_PLUGIN :::: Define metrics for exec_id: %s" % (exec_id))

            if 'rddBlocks' in executor and executor['rddBlocks'] > 0:
                self.results_builder.add_absolute_result(PluginMeasurement(key='rddBlocks', value=executor['rddBlocks'],
                                                                           dimensions={"app_name": app_name,
                                                                                       "host_name": host},
                                                                           entity_selector=entity_selector))
            if 'memoryUsed' in executor and executor['memoryUsed'] > 0:
                self.results_builder.add_absolute_result(PluginMeasurement(key='memoryUsed', value=executor['memoryUsed'],
                                                                           dimensions={"app_name": app_name,
                                                                                       "host_name": host},
                                                                           entity_selector=entity_selector))
            if 'diskUsed' in executor and executor['diskUsed'] > 0:
                self.results_builder.add_absolute_result(PluginMeasurement(key='diskUsed', value=executor['diskUsed'],
                                                                           dimensions={"app_name": app_name,
                                                                                       "host_name": host},
                                                                           entity_selector=entity_selector))
            if 'totalCores' in executor and executor['totalCores'] > 0:
                self.results_builder.add_absolute_result(PluginMeasurement(key='totalCores', value=executor['totalCores'],
                                                                           dimensions={"app_name": app_name,
                                                                                       "host_name": host},
                                                                           entity_selector=entity_selector))
            if 'maxTasks' in executor and executor['maxTasks'] > 0:
                self.results_builder.add_absolute_result(PluginMeasurement(key='maxTasks', value=executor['maxTasks'],
                                                                           dimensions={"app_name": app_name,
                                                                                       "host_name": host},
                                                                           entity_selector=entity_selector))
            if 'activeTasks' in executor and executor['activeTasks'] > 0:
                self.results_builder.add_absolute_result(PluginMeasurement(key='activeTasks', value=executor['activeTasks'],
                                                                           dimensions={"app_name": app_name,
                                                                                       "host_name": host},
                                                                           entity_selector=entity_selector))
            if 'failedTasks' in executor and executor['failedTasks'] > 0:
                self.results_builder.add_absolute_result(PluginMeasurement(key='failedTasks', value=executor['failedTasks'],
                                                                           dimensions={"app_name": app_name,
                                                                                       "host_name": host},
                                                                           entity_selector=entity_selector))
            if 'completedTasks' in executor and executor['completedTasks'] > 0:
                self.results_builder.add_absolute_result(
                    PluginMeasurement(key='completedTasks', value=executor['completedTasks'],
                                      dimensions={"app_name": app_name, "host_name": host},
                                      entity_selector=entity_selector))
            if 'totalTasks' in executor and executor['totalTasks'] > 0:
                self.results_builder.add_absolute_result(PluginMeasurement(key='totalTasks', value=executor['totalTasks'],
                                                                           dimensions={"app_name": app_name,
                                                                                       "host_name": host},
                                                                           entity_selector=entity_selector))
            if 'totalDuration' in executor and executor['totalDuration'] > 0:
                self.results_builder.add_absolute_result(
                    PluginMeasurement(key='totalDuration', value=executor['totalDuration'],
                                      dimensions={"app_name": app_name, "host_name": host},
                                      entity_selector=entity_selector))
            if 'totalGCTime' in executor and executor['totalGCTime'] > 0:
                self.results_builder.add_absolute_result(PluginMeasurement(key='totalGCTime', value=executor['totalGCTime'],
                                                                           dimensions={"app_name": app_name,
                                                                                       "host_name": host},
                                                                           entity_selector=entity_selector))
            if 'totalInputBytes' in executor and executor['totalInputBytes'] > 0:
                self.results_builder.add_absolute_result(
                    PluginMeasurement(key='totalInputBytes', value=executor['totalInputBytes'],
                                      dimensions={"app_name": app_name, "host_name": host},
                                      entity_selector=entity_selector))
            if 'totalShuffleRead' in executor and executor['totalShuffleRead'] > 0:
                self.results_builder.add_absolute_result(
                    PluginMeasurement(key='totalShuffleRead', value=executor['totalShuffleRead'],
                                      dimensions={"app_name": app_name, "host_name": host},
                                      entity_selector=entity_selector))
            if 'totalShuffleWrite' in executor and executor['totalShuffleWrite'] > 0:
                self.results_builder.add_absolute_result(
                    PluginMeasurement(key='totalShuffleWrite', value=executor['totalShuffleWrite'],
                                      dimensions={"app_name": app_name, "host_name": host},
                                      entity_selector=entity_selector))
            if 'maxMemory' in executor and executor['maxMemory'] > 0:
                self.results_builder.add_absolute_result(PluginMeasurement(key='maxMemory', value=executor['maxMemory'],
                                                                           dimensions={"app_name": app_name,
                                                                                       "host_name": host},
                                                                           entity_selector=entity_selector))

            # Not all jobs has it's memory metrics, in such case this key may be missing
            if 'memoryMetrics' in executor:
                if 'usedOnHeapStorageMemory' in executor['memoryMetrics'] and executor['memoryMetrics']['usedOnHeapStorageMemory']:
                    self.results_builder.add_absolute_result(PluginMeasurement(key='usedOnHeapStorageMemory',
                                                                               value=executor['memoryMetrics'][
                                                                                   'usedOnHeapStorageMemory'],
                                                                               dimensions={"app_name": app_name,
                                                                                           "host_name": host},
                                                                               entity_selector=entity_selector))
                if 'usedOffHeapStorageMemory' in executor['memoryMetrics'] and executor['memoryMetrics']['usedOffHeapStorageMemory'] > 0:
                    self.results_builder.add_absolute_result(PluginMeasurement(key='usedOffHeapStorageMemory',
                                                                               value=executor['memoryMetrics'][
                                                                                   'usedOffHeapStorageMemory'],
                                                                               dimensions={"app_name": app_name,
                                                                                           "host_name": host},
                                                                               entity_selector=entity_selector))
                if 'totalOnHeapStorageMemory' in executor['memoryMetrics'] and executor['memoryMetrics']['totalOnHeapStorageMemory'] > 0:
                    self.results_builder.add_absolute_result(PluginMeasurement(key='totalOnHeapStorageMemory',
                                                                               value=executor['memoryMetrics'][
                                                                                   'totalOnHeapStorageMemory'],
                                                                               dimensions={"app_name": app_name,
                                                                                           "host_name": host},
                                                                               entity_selector=entity_selector))
                if 'totalOffHeapStorageMemory' in executor['memoryMetrics'] and executor['memoryMetrics']['totalOffHeapStorageMemory'] > 0:
                    self.results_builder.add_absolute_result(PluginMeasurement(key='totalOffHeapStorageMemory',
                                                                               value=executor['memoryMetrics'][
                                                                                   'totalOffHeapStorageMemory'],
                                                                               dimensions={"app_name": app_name,
                                                                                           "host_name": host},
                                                                               entity_selector=entity_selector))

    def str_to_date(self, date_str):
        date_time_str = date_str[:len(date_str) - 7]
        return datetime.strptime(date_time_str, '%Y-%m-%dT%H:%M:%S')

    def filter_apps(self, json):
        json_obj_filtered = []
        time_delta = timedelta(minutes=int(self.config['end_time_filter']))

        minEndDateTime = datetime.now() - time_delta
        self.logger.info("SPICA_SPARK_PLUGIN :::: FILTER Custom param key: %s, Custom param value: %s" % (
            'minEndDateTime', minEndDateTime))

        for app in json:
            filtered_app = {}
            filtered_attempts = []
            for attempt in app['attempts']:
                start_time = self.str_to_date(attempt['startTime'])
                end_time = self.str_to_date(attempt['endTime'])
                completed = attempt['completed']
                if (not completed and attempt['endTime'] == '1969-12-31T23:59:59.999GMT' and start_time > minEndDateTime) or (completed and (start_time > minEndDateTime or end_time > minEndDateTime)):
                    filtered_attempts.append(attempt)

            if len(filtered_attempts) > 0:
                filtered_app['id'] = app['id']
                filtered_app['name'] = app['name']
                filtered_app['attempts'] = filtered_attempts
                json_obj_filtered.append(filtered_app)
        return json_obj_filtered

    def get_job_status(self, status):
        return_value = 0
        if status == 'RUNNING':
            return_value = 1
        elif status == 'SUCCEEDED':
            return_value = 2
        elif status == 'FAILED':
            return_value = -1
        return return_value
