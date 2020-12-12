import os
import sys
import json

from .common import getDateDiffMs, dumpJson
from .schema import *

class Formatter():
    def getFormattedEvent(self, event):
        pass

    def _getFlattenedEventData(self, schema, inputData):
        data = {}
        for key in schema:
            value = schema[key]
            if isinstance(value, str):
                if key in inputData:
                    data[value] = inputData[key]
            elif isinstance(value, dict):
                if key in inputData:
                    data.update(self._getFlattenedEventData(value, inputData[key]))
            elif isinstance(value, list):
                if len(value) > 0:
                    if isinstance(value[0], dict):
                        listSchema = value[0]
                        if key in inputData:
                            data[key] = []
                            for item in inputData[key]:
                                data[key].append(self._getFlattenedEventData(listSchema, item))
                    elif isinstance(value[0], str):
                        if key in inputData:
                            for item in inputData[key]:
                                pass  ### NOT IMPLEMENTED
            else:
                print('Skipping: {}'.format(value))
        return data

class TaskFormatter(Formatter):
    def getFormattedEvent(self, event):
        task = {}
        schemaType = event['Event']
        task = self._getFlattenedEventData(taskSchema[schemaType], event)
        if schemaType == 'SparkListenerTaskEnd':
            if 'Task End Reason' in event:
                task['EndReasonFULL'] = event['Task End Reason']

            if 'LaunchTime' in task and 'FinishTime' in task and 'EndReason' in task and task['EndReason'] == 'Success':
                totalExecutionTime = int(task['FinishTime']) - int(task['LaunchTime'])
                executorOverhead = int(task.get('ExecutorDeserializeTime',0)) + int(task.get('ResultSerializationTime',0))

                if 'GettingResultTime' in task and task['GettingResultTime'] > task['LaunchTime']:
                    ### GettingResultTime is in Epoc time at which it started; get duration by subtracting from endtime
                    ### May need to look at SparkListenerTaskGettingResult
                    task['GettingResultTime'] = int(task['FinishTime']) - int(task['GettingResultTime'])
                else:
                    task['GettingResultTime'] = 0
                task['Duration'] = totalExecutionTime
                task['SchedulerDelay'] = max(0,
                        (task['Duration'] - int(task.get('ExecutionDuration',0)) - executorOverhead - int(task['GettingResultTime'])))
            else:
                task['Duration'] = 0
                task['SchedulerDelay'] = 0
            if 'ShuffleWriteTime' in task:
                ### ShuffleWriteTime is in Nanoseconds; convert to millisecs
                task['ShuffleWriteTime'] = int(task['ShuffleWriteTime']/1000000)
        return task

class StageFormatter(Formatter):
    def getAccumulablesValue(self, nameTuples, accumulablesDict):
        result = {}
        for item in accumulablesDict:
            if item['Name'] in nameTuples:
                result[nameTuples[item['Name']]] = item['Value']
        return result

    def getRDDList(self, rddInfos, stageId):
        rdds = []
        for rddInfo in rddInfos:
            rdd = {}
            rdd['Id'] = rddInfo['RDD ID']
            rdd['Name'] = rddInfo['Name']
            if 'Scope' in rddInfo:
                rddScope = json.loads(rddInfo['Scope'])
                rdd['ScopeId'] = rddScope['id']
                rdd['ScopeName'] = rddScope['name']
            else:
                rdd['ScopeId'] = ''
                rdd['ScopeName'] = ''
            rdd['Callsite'] = rddInfo['Callsite']
            rdd['Partitions'] = rddInfo['Number of Partitions']
            rdd['CachedPartitions'] = rddInfo['Number of Cached Partitions']
            rdd['MemorySize'] = rddInfo['Memory Size']
            rdd['DiskSize'] = rddInfo['Disk Size']
            rdds.append(rdd)
        return sorted(rdds, key=lambda x: x['Id'])

    def getFormattedEvent(self, event):
        stage = {}
        stage = self._getFlattenedEventData(stageSchema[event['Event']], event)
        stage.update(self._getFlattenedEventData(stageSchema['derivedSchema'], event))

        if 'Stage Info' in event and 'RDD Info' in event['Stage Info']:
            stage['RDDs'] = self.getRDDList(event['Stage Info']['RDD Info'], stage['Id'])
        if 'SubmissionTime' in stage and 'CompletionTime' in stage:
            stage['Duration'] = getDateDiffMs(stage['SubmissionTime'], stage['CompletionTime'])
        stage['Status'] = 'Submitted'
        if 'CompletionTime' in stage:
            stage['Status'] = 'Completed'
        return stage

    def getStageDetailsFromList(self, stageInfosList):
        stages = []
        for stageInfo in stageInfosList:
            stageEvent = { 'Event': 'SparkListenerStageSubmitted', 'Stage Info': stageInfo }
            stage = self.getFormattedEvent(stageEvent)
            stage['Status'] = 'Skipped'
            stages.append(stage)
        return stages

class JobFormatter(Formatter):
    def getFormattedEvent(self, event):
        job = {}
        type = event['Event']
        job = self._getFlattenedEventData(jobSchema[type], event)
        job.update(self._getFlattenedEventData(jobSchema['derivedSchema'], event))
        if 'Stage Infos' in event and len(event['Stage Infos']) > 0:
            if 'Stage Name' in event['Stage Infos'][0]:
                job['FirstStageName'] = event['Stage Infos'][0]['Stage Name']
        return job

class SqlExecutionFormater(Formatter):
    def getFormattedEvent(self, event):
        type = event['Event']
        sqlPlan =  self._getFlattenedEventData(sqlExecutionSchema[type], event)
        return sqlPlan

class FormatterFactory():
    taskFormatter = TaskFormatter()
    stageFormatter = StageFormatter()
    jobFormatter = JobFormatter()
    sqlExecutionFormater = SqlExecutionFormater()

    def getFormattedEvent(self, event):
        eventType = event['Event']
        if eventType == 'SparkListenerTaskStart' or eventType == 'SparkListenerTaskEnd':
            return ('Task', self.taskFormatter.getFormattedEvent(event))
        elif eventType == 'SparkListenerStageSubmitted' or eventType == 'SparkListenerStageCompleted':
            return ('Stage', self.stageFormatter.getFormattedEvent(event))
        elif eventType == 'SparkListenerJobStart' or eventType == 'SparkListenerJobEnd':
            formattedJob = self.jobFormatter.getFormattedEvent(event)
            if 'Stage Infos' in event:
                formattedJob['Stages'] = self.stageFormatter.getStageDetailsFromList(event['Stage Infos'])
            return ('Job', formattedJob)
        elif eventType == 'org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionStart':
            #dumpJson(event, 'sqlExplainPlan_{}.json'.format(event['executionId']), './SQLExplainPLans')
            formattedPLan = self.sqlExecutionFormater.getFormattedEvent(event)
            return ('SQLPlan', formattedPLan )
        else:
            return (eventType, None)
