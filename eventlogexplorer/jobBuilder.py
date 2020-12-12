import json

from .databricksJob import DatabricksJob
from .common import getDateDiffMs, DatabricksJobInfo

class JobBuilder:
    def __init__(self, reader):
        self.reader = reader
        self.jobId = reader.jobId
        self.runId = reader.runId
        self.isJobCluster = reader.isJobCluster
        self.clusterId = None
        self.driverNode = None
        self.workerNode = None
        self.sparkVersion = None
        self.scalingType = None
        self.noOfNodes = None
        self.executorMemory = None
        self.jobIdProp = 'spark.databricks.job.id'
        self.runIdProp = 'spark.databricks.job.runId'
        self.commonProperties = {
            "spark.databricks.driverNodeTypeId",
            "spark.databricks.workerNodeTypeId",
            "spark.executor.memory",
            "spark.databricks.clusterUsageTags.clusterScalingType",
            "spark.databricks.clusterUsageTags.clusterId",
            "spark.databricks.clusterUsageTags.clusterName",
            "spark.databricks.clusterUsageTags.sparkVersion",
            "spark.databricks.clusterUsageTags.region",
            "spark.databricks.clusterUsageTags.dataPlaneRegion",
            "spark.databricks.clusterUsageTags.clusterTargetWorkers",
            "spark.databricks.clusterUsageTags.clusterMinWorkers",
            "spark.databricks.clusterUsageTags.clusterMaxWorkers",
            "spark.databricks.clusterUsageTags.clusterWorkers",
            "spark.databricks.api.url",
            "spark.executor.memory"
        }
        self.specificProperties = {
            "PYTHON_USERNAME",
            "__is_continuous_processing",
            "callSite.long",
            "callSite.short",
            "orgId",
            "spark.databricks.delta.retentionDurationCheck.enabled",
            "spark.databricks.isolationID",
            "spark.databricks.job.id",
            "spark.databricks.job.runId",
            "spark.databricks.replId",
            "spark.databricks.token",
            "spark.databricks.notebook.id",
            "spark.databricks.notebook.path",
            "spark.job.description",
            "spark.jobGroup.id",
            "spark.rdd.scope",
            "spark.rdd.scope.noOverride",
            "spark.scheduler.pool",
            "spark.sql.adaptive.enabled",
            "spark.sql.cbo.enabled",
            "spark.sql.execution.id",
            "spark.sql.execution.parent",
            "spark.sql.shuffle.partitions",
            "spark.sql.streaming.aggregation.stateFormatVersion",
            "spark.sql.streaming.flatMapGroupsWithState.stateFormatVersion",
            "spark.sql.streaming.multipleWatermarkPolicy",
            "spark.sql.streaming.stateStore.providerClass",
            "sql.streaming.queryId",
            "streaming.sql.batchId",
            "user",
            "userId"
        }

    def getModifiedProperties(self, props, commonProps):
        specificProps = {}
        if not commonProps:
            for prop in props:
                if prop not in self.specificProperties:
                    commonProps[prop] = props[prop]
                else:
                    specificProps[prop] = props[prop]
        else:
            for prop in props:
                if prop in self.commonProperties:
                    commonProps[prop] = props[prop]
                elif prop in self.specificProperties or prop not in commonProps or props[prop] != commonProps[prop]:
                    specificProps[prop] = props[prop]
        return specificProps

    def storeTaskIdInStage(self, task, stages):
        if 'StageId' in task and task['StageId'] in stages:
            stage = stages[task['StageId']][task['StageAttemptId']]
            if 'TaskIds' not in stage:
                stage['TaskIds'] = []
            if task['Id'] not in stage['TaskIds']:
                stage['TaskIds'].append(task['Id'])
            return True
        return False

    def storeTaskEvent(self, event, tasks):
        if 'Id' in event:
            taskId = event['Id']
            if taskId in tasks:
                tasks[taskId].update(event)
            else:
                tasks[taskId] = event
            return tasks[taskId]
        return None

    def storeStageEvent(self, event, stages):
        if 'Id' in event and 'StageAttemptId' in event:
            stageId, attemptId = event['Id'], event['StageAttemptId']
            if stageId in stages:
                if  attemptId in stages[stageId]:
                    stages[stageId][attemptId].update(event)
                else:
                    stages[stageId][attemptId] = event
            else:
                stages[stageId] = { attemptId: event }
        return None

    def storeJobEvent(self, event, jobs):
        if 'Id' in event:
            jobId = event['Id']
            if jobId in jobs:
                jobs[jobId].update(event)
            else:
                jobs[jobId] = event
            return jobs[jobId]
        return None

    def isJobFiltered(self, event):
        if self.isJobCluster:
            return True
        if 'Properties' in event:
            if (event['Properties'].get(self.jobIdProp, '-1') == self.jobId and
               event['Properties'].get(self.runIdProp, '-1') == self.runId):
                return True
        return False

    def processEvent(self, type, formattedEvent, dbJob):
        if type == 'Task':
            if 'StageAttemptId' in formattedEvent and formattedEvent['StageAttemptId'] == -1:
                ## TODO: Found an example of negative attempt Id
                print('Found negative Stage Attempt Id ...')
                print(formattedEvent)
                return
            if 'StageId' in formattedEvent:
                if self.storeTaskIdInStage(formattedEvent, dbJob['Stages']):
                    updatedTask = self.storeTaskEvent(formattedEvent, dbJob['Tasks'])
        if type == 'Stage':
            if 'Id' in formattedEvent and formattedEvent['Id'] in dbJob['Stages']:
                updatedStage = self.storeStageEvent(formattedEvent, dbJob['Stages'])
        if type == 'Job':
            if 'Properties' in formattedEvent:
                formattedEvent['Properties'] = self.getModifiedProperties(formattedEvent['Properties'], dbJob['Properties'])
            if self.isJobFiltered(formattedEvent) or formattedEvent['Id'] in dbJob['Jobs']:
                if 'Stages' in formattedEvent:
                    for stage in formattedEvent['Stages']:
                        self.storeStageEvent(stage, dbJob['Stages'])
                updatedEvent = self.storeJobEvent(formattedEvent, dbJob['Jobs'])
                ### No Submission time in Job End Listener
                if updatedEvent and 'SubmissionTime' in updatedEvent and 'CompletionTime' in updatedEvent:
                    updatedEvent['Duration'] = getDateDiffMs(updatedEvent['SubmissionTime'], updatedEvent['CompletionTime'])
                else:
                    updatedEvent['Duration'] = 0

    def getDatabricksJobAsJson(self):
        dbJob = { 'Jobs': {}, 'Stages': {}, 'Tasks': {}, 'Properties': {} }
        for type, formattedEvent in self.reader.read():
            self.processEvent(type, formattedEvent, dbJob)
        noJobsBeforeCleaning = len(dbJob['Jobs'])
        print('Processed: {} jobs, {} stages and {} tasks'.format(len(dbJob['Jobs']), len(dbJob['Stages']), len(dbJob['Tasks'])))

        ## Remove partial jobs without Starting event
        for id in [id for id in dbJob['Jobs'] if 'SubmissionTime' not in dbJob['Jobs'][id]]: del dbJob['Jobs'][id] 
        if len(dbJob['Jobs']) < noJobsBeforeCleaning:
            print('Removed {} jobs with missing submission time.'.format(noJobsBeforeCleaning - len(dbJob['Jobs'])))
        return dbJob
        
    def getDatabricksJob(self):
        dbJob = self.getDatabricksJobAsJson()
        if 'Properties' in dbJob:
            if self.clusterId is None:
                self.clusterId = dbJob['Properties'].get('spark.databricks.clusterUsageTags.clusterId', None)
            self.driverNode = dbJob['Properties'].get('spark.databricks.driverNodeTypeId', None)
            self.workerNode = dbJob['Properties'].get('spark.databricks.workerNodeTypeId', None)
            self.sparkVersion = dbJob['Properties'].get('spark.databricks.clusterUsageTags.sparkVersion', None)
            self.scalingType = dbJob['Properties'].get('spark.databricks.clusterUsageTags.clusterScalingType', None)
            self.noOfNodes = dbJob['Properties'].get('spark.databricks.clusterUsageTags.clusterTargetWorkers', None)
            self.executorMemory = dbJob['Properties'].get('spark.executor.memory', None)
        dbJobInfo = DatabricksJobInfo(self.jobId, self.runId, self.clusterId, self.sparkVersion, self.driverNode, self.workerNode,
                                      self.scalingType, self.noOfNodes, self.executorMemory)
        return DatabricksJob(dbJobInfo, dbJob)

