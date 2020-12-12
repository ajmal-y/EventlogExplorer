import json
from .template import Template
from .common import MetricsAccumulator, DistributionMetics, TaskStats, StageStats, JobStats, JobDetails, StageDetails, DatabricksJobInfo, StageFailures, distMetricsItems, distMetricsHeader, zipLocalFilesAndCopyToTarget

class PerfTest:
    def getPerfJobTestData(jobData, start, count):
        bigJobData = []
        for id in range(start, count):
            element = copy.deepcopy(random.choice(jobData))
            element.jobId = id
            bigJobData.append(element)
        return bigJobData

    def getPerfStageTestData(stageData, start, count):
        bigStageData = []
        for id in range(start, count):
            element = copy.deepcopy(random.choice(stageData))
            element.stageId = id
            bigStageData.append(element)
        return bigStageData

class DatabricksJob:
    def __init__(self, jobInfo, jsonData):
        self.jobInfo = jobInfo
        self.jsonData = jsonData
        self.startTime = 0
        self.endTime = 0
        self._jobsPageList = []
        self._stageAttemptsPageList = []
    def __writeToFile(self, filename, dataStr):
        try:
            with open(filename, 'w') as fw:
                fw.write(dataStr)
        except: ### On Python2
            import io
            with io.open(filename, 'w', encoding='utf8') as fw:
                fw.write(dataStr)

    ### TODO: Use this to explore failure reasons
    def _printTaskFailureReason(self, task):
        print(task['Id'], task['StageId'], task['Attempt'], task['ExecutorId'], task['EndReason'])
        if 'EndReasonFULL' in task:
            print(task['EndReasonFULL'])

    def getAggregatedTaskStatsAndMetrics(self, jobId, stageAttempt, allTasks, prevFailedTaskIndexes):
        stageAttemptLevelTaskStats = TaskStats()
        stageAttemptLevelStageStats = StageStats()
        stageAttemptLevelMetrics = MetricsAccumulator()
        attemptedTasks = []
        currFailedTaskIndexes = set()
        failedReasonList = {}

        if 'StageAttemptId' in stageAttempt and stageAttempt['StageAttemptId'] == 0:
            if 'TaskIds' not in stageAttempt:   ## TaskIds wont be present in Skipped Stage
                stageAttemptLevelStageStats.skippedStages = 1
                stageAttemptLevelTaskStats.skippedTasks = stageAttempt['NumberOfTasks']
                return stageAttemptLevelTaskStats, stageAttemptLevelStageStats, stageAttemptLevelMetrics, currFailedTaskIndexes
            stageAttemptLevelStageStats.requiredStages = 1
            stageAttemptLevelTaskStats.requiredTasks = stageAttempt['NumberOfTasks']
        else:
            stageAttemptLevelStageStats.retriedStages = 1

        for taskId in stageAttempt['TaskIds']:
            if taskId in allTasks:
                task = allTasks[taskId]
                attemptedTasks.append(task)
                stageAttemptLevelTaskStats.accumulateTaskStats(task)
                if 'EndReason' in task:
                    if task['EndReason'] == 'Success':
                        if prevFailedTaskIndexes is None or task['Index'] in prevFailedTaskIndexes:
                            stageAttemptLevelMetrics.accumulateTaskMetrics(task)
                        currFailedTaskIndexes.discard(task['Index'])
                    else:
                        ####self._printTaskFailureReason(task)
                        failedReasonList[task['EndReason']] = failedReasonList.get(task['EndReason'],0) + 1
                else:
                    failedReasonList['Unknown Failure'] = failedReasonList.get('Unknown Failure',0) + 1
        if currFailedTaskIndexes:
            stageAttemptLevelStageStats.failedStages = 1
        else:
            stageAttemptLevelStageStats.succeededStages = 1
        finalFailedList = []
        for reason in failedReasonList:
            finalFailedList.append(StageFailures(reason, failedReasonList[reason]))
        self._stageAttemptsPageList.append(
                    StageDetails(jobId, stageAttempt, stageAttemptLevelTaskStats, stageAttemptLevelStageStats,
                    stageAttemptLevelMetrics, DistributionMetics(attemptedTasks), finalFailedList)
        )
        return stageAttemptLevelTaskStats, stageAttemptLevelStageStats, stageAttemptLevelMetrics, currFailedTaskIndexes

    def getAggregatedStageStatsAndMetrics(self, jobId, stageAttempts, allTasks):
        stageLevelTaskStats = TaskStats()
        stageLevelStageStats = StageStats()
        stageLevelMetrics = MetricsAccumulator()

        failedTaskIndexes = None
        for attemptId in sorted(stageAttempts):
            stageAttempt = stageAttempts[attemptId]
            if 'TaskIds' in stageAttempt and stageAttempt['TaskIds']:
                stageAttempt = stageAttempts[attemptId]
                stageAttemptLevelTaskStats, stageAttemptLevelStageStats, stageAttemptLevelMetrics, failedTaskIndexes = (
                    self.getAggregatedTaskStatsAndMetrics(jobId, stageAttempt, allTasks, failedTaskIndexes)
                )
                stageLevelTaskStats.accumulate(stageAttemptLevelTaskStats)
                stageLevelStageStats.accumulate(stageAttemptLevelStageStats)
                stageLevelMetrics.accumulate(stageAttemptLevelMetrics)
        return stageLevelTaskStats, stageLevelStageStats, stageLevelMetrics

    def getAggregatedJobStatsAndMetrics(self, job, allStages, allTasks):
        jobLevelTaskStats = TaskStats()
        jobLevelStageStats = StageStats()
        jobLevelMetrics = MetricsAccumulator()

        for stageId in job['StageIds']:
            if stageId in allStages:
                stageLevelTaskStats, stageLevelStageStats, stageLevelMetrics = (
                    self.getAggregatedStageStatsAndMetrics(job['Id'], allStages[stageId], allTasks)
                )
                jobLevelTaskStats.accumulate(stageLevelTaskStats)
                jobLevelStageStats.accumulate(stageLevelStageStats)
                jobLevelMetrics.accumulate(stageLevelMetrics)
        self._jobsPageList.append(JobDetails(job, jobLevelTaskStats, jobLevelStageStats, jobLevelMetrics))
        return jobLevelTaskStats, jobLevelStageStats, jobLevelMetrics

    def getDbJobStatsAndMetrics(self):
        dbJobStats = JobStats()
        dbStageStats = StageStats()
        dbTaskStats = TaskStats()
        dbJobMetrics = MetricsAccumulator()

        self.startTime, self.endTime = 0, 0
        self._jobsPageList, self._stageAttemptsPageList = [], []
        for jobId in self.jsonData['Jobs']:
            job = self.jsonData['Jobs'][jobId]
            if self.startTime == 0 or ('SubmissionTime' in job and job['SubmissionTime'] < self.startTime):
                self.startTime = job['SubmissionTime']
            if 'CompletionTime' in job and job['CompletionTime'] > self.endTime:
                self.endTime = job['CompletionTime']
            if 'StageIds' in job:
                jobLevelTaskStats, jobLevelStageStats, jobLevelMetrics = (
                    self.getAggregatedJobStatsAndMetrics(job, self.jsonData['Stages'], self.jsonData['Tasks'])
                )
            dbJobStats.accumulate(job)
            dbStageStats.accumulate(jobLevelStageStats)
            dbTaskStats.accumulate(jobLevelTaskStats)
            dbJobMetrics.accumulate(jobLevelMetrics)
        self.jobInfo.setTime(self.startTime, self.endTime)
        return self.jobInfo, dbJobStats, dbStageStats, dbTaskStats, dbJobMetrics, self._jobsPageList, self._stageAttemptsPageList

    def generateReport(self):
        if not self.jsonData['Jobs']:
            return '<p style="color:red;">No data found! Skipping report...</p>'
        reportFiles = []
        jobInfo, jobStats, jobStageStats, jobTaskStats, jobMetrics, jobData, stageData = self.getDbJobStatsAndMetrics()

        #jobData = PerfTest.getPerfTestData(jobData, 1000, 11000)
        #stageData = PerfTest.getPerfTestData(stageData, 1000, 11000)

        #startTime = min(jobData, key=lambda x: if x x.subTm)
        #endTime = max(jobData, key=lambda x: x.comTm)

        title = jobInfo.titleTemplate().format('jobs')
        jobData.sort(key=lambda x:x.jobId, reverse=False)
        jobData = [x.toJson() for x in jobData]
        template = Template().get('jobs_page.j2')
        result = template.render(title=title, jobInfo=jobInfo, jobStats=jobStats, stageStats=jobStageStats,
                                 taskStats=jobTaskStats, metrics=jobMetrics, tableData=jobData)
        htmlFile = '/tmp/job-{}-run-{}_cluster-{}_JOBS.html'.format(jobInfo.dbJobId, jobInfo.dbRunId, jobInfo.clusterId)
        reportFiles.append(htmlFile)
        self.__writeToFile(htmlFile, result)

        title = jobInfo.titleTemplate().format('stages')
        stageDataJson = { "data": [x.toJson() for x in stageData], "distMetricsItems": distMetricsItems, "distMetricsHeader": distMetricsHeader }
        #stageDataJson = { "data": [x.toList() for x in stageData], "distMetricsItems": distMetricsItems, "distMetricsHeader": distMetricsHeader }
        template = Template().get('stages_page.j2')
        result = template.render(title=title, jobInfo=jobInfo, tableData=stageDataJson)
        htmlFile = '/tmp/job-{}-run-{}_cluster-{}_STAGES.html'.format(jobInfo.dbJobId, jobInfo.dbRunId, jobInfo.clusterId)
        reportFiles.append(htmlFile)
        self.__writeToFile(htmlFile, result)

        targetZipFile = zipLocalFilesAndCopyToTarget(reportFiles)
        print('Created the zipfile:', targetZipFile)
        return '<a href={} download>Download eventlog report: jobReport.zip</a>'.format(targetZipFile)

