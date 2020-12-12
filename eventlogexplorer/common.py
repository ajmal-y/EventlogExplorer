##################################
### Common/Utility Definitions ###
##################################

import os
import json
import math
import time
#import statistics
import functools
import zipfile
import tempfile
from shutil import copyfile
from datetime import datetime
try:
    from datetime import timezone
    version = '3.x'
except:
    version = '2.x'

class DatabricksJobInfo:
    def __init__(self, dbJobId, dbRunId, clusterId, sparkVersion='-', driverNode='-', workerNode='-',
                 scalingType='-', noOfNodes='-', executorMemory='-'):
        self.generatedOn = time.strftime('%d %b %Y %I:%M %p %Z') ## '%a, %d'
        self.dbJobId = dbJobId
        self.dbRunId = dbRunId
        self.clusterId = clusterId
        self.sparkVersion = sparkVersion
        self.driverNode = driverNode
        self.workerNode = workerNode
        self.scalingType = scalingType
        self.noOfNodes = noOfNodes
        self.executorMemory = executorMemory
        self.startTime = '-'
        self.endTime = '-'
    def setTime(self, startTime, endTime):
        self.startTime = toDT(startTime)
        self.endTime = toDT(endTime)
        self.duration = getDateDiffMs(startTime, endTime)
    def titleTemplate(self):
        if self.dbJobId and self.dbRunId:
            return 'Spark {} for the Databrics Job-Run: ' + self.dbJobId + '-' + self.dbRunId
        elif self.clusterId:
            return 'Spark {} for the cluster: ' + self.clusterId
        else:
            return 'Spark {}'
    def toJson(self):
        return self.__dict__

class Distribution:
    def __init__(self, min, p25, median, p75, max, mean, skew):
        self.min = min
        self.p25 = p25
        self.med = median
        self.p75 = p75
        self.max = max
        self.mea = mean
        self.skew = skew
    def toJson(self):
        return self.__dict__
    def toList(self):
        return [self.min, self.p25, self.med, self.p75, self.max, self.mea, self.skew]

def prcntl(N, P):
    if not N:
        return 0
    n = int(round(P * len(N) + 0.5))
    return N[n-1]

class DistributionMetics:
    def _calculate(self, values):
        vals = sorted(values)
        min, p25, med, p75, max, mean, skew = (
            getMin(vals), prcntl(vals,0.25), prcntl(vals,0.5), prcntl(vals,0.75), getMax(vals), getMean(vals), getSkew(values)
        )
        return Distribution(min, p25, med, p75, max, mean, skew)
    def __init__(self, tasks):
        values = [task['Duration'] for task in tasks if 'Duration' in task]
        self.duration = self._calculate(values)
        values = [task['GCTime'] for task in tasks if 'GCTime' in task]
        self.gcTime = self._calculate(values)
        values = [task['ExecutorDeserializeTime'] for task in tasks if 'ExecutorDeserializeTime' in task]
        self.taskDeserializationTime = self._calculate(values)
        values = [task['ResultSerializationTime'] for task in tasks if 'ResultSerializationTime' in task]
        self.resultSerializationTime = self._calculate(values)
        values = [task['ShuffleFetchWaitTime'] for task in tasks if 'ShuffleFetchWaitTime' in task]
        self.shuffleFetchWaitTime = self._calculate(values)
        values = [task['ShuffleWriteTime'] for task in tasks if 'ShuffleWriteTime' in task]
        self.shuffleWriteTime = self._calculate(values)
        values = [task['SchedulerDelay'] for task in tasks if 'SchedulerDelay' in task]
        self.schedulerDelay = self._calculate(values)
        values = [task['GettingResultTime'] for task in tasks if 'GettingResultTime' in task]
        self.gettingResultTime = self._calculate(values)
        values = [task['InputRecords'] for task in tasks if 'InputRecords' in task]
        self.inputRecords = self._calculate(values)
        values = [task['OutputRecords'] for task in tasks if 'OutputRecords' in task]
        self.outputRecords = self._calculate(values)
        values = [task['InputSize'] for task in tasks if 'InputSize' in task]
        self.inputSize = self._calculate(values)
        values = [task['OutputSize'] for task in tasks if 'OutputSize' in task]
        self.outputSize = self._calculate(values)
        values = [task['ShuffleTotalRecordsRead'] for task in tasks if 'ShuffleTotalRecordsRead' in task]
        self.shuffleReadRecords = self._calculate(values)
        values = [task['ShuffleWriteRecords'] for task in tasks if 'ShuffleWriteRecords' in task]
        self.shuffleWriteRecords = self._calculate(values)
        values = [task['ShuffleRemoteBytesRead'] for task in tasks if 'ShuffleRemoteBytesRead' in task]
        self.shuffleRemoteReadSize = self._calculate(values)
        values = [task['ShuffleLocalBytesRead'] for task in tasks if 'ShuffleLocalBytesRead' in task]
        self.shuffleLocalReadSize = self._calculate(values)
        values = [task['ShuffleRemoteBytesReadToDisk'] for task in tasks if 'ShuffleRemoteBytesReadToDisk' in task]
        self.shuffleLocalReadToDiskSize = self._calculate(values)
        values = [task['ShuffleWriteSize'] for task in tasks if 'ShuffleWriteSize' in task]
        self.shuffleWriteSize = self._calculate(values)
    def toJson(self):
        return {
            'duTm' : self.duration.toList(),
            'sDTm' : self.schedulerDelay.toList(),
            'tDTm' : self.taskDeserializationTime.toList(),
            'gcTm' : self.gcTime.toList(),
            'rSTm' : self.resultSerializationTime.toList(),
            'gRTm' : self.gettingResultTime.toList(),
            'sWTm' : self.shuffleWriteTime.toList(),
            'iSz' : self.inputSize.toList(),
            'iRc' : self.inputRecords.toList(),
            'oSz' : self.outputSize.toList(),
            'oRc' : self.outputRecords.toList(),
            'sFWTm' : self.shuffleFetchWaitTime.toList(),
            'sRSz' : self.shuffleLocalReadSize.toList(),
            'sRDSz' : self.shuffleLocalReadToDiskSize.toList(),
            'sRRc' : self.shuffleReadRecords.toList(),
            'sRSz' : self.shuffleRemoteReadSize.toList(),
            'sWSz' : self.shuffleWriteSize.toList(),
            'sWRc' : self.shuffleWriteRecords.toList()
        }

distMetricsHeader = [
    "Metric",
    "Min",
    "25th Percentile",
    "Median",
    "75th Percentile",
    "Max",
    "Mean",
    "Skewness"
]
distMetricsItems = {
    "duTm":"Duration",
    "sDTm":"Scheduler Delay",
    "tDTm":"Task Deserialization Time",
    "gcTm":"GC Time",
    "rSTm":"Result Serialization Time",
    "gRTm":"Getting Result Time",
    "sWTm":"Shuffle Write Time",
    "iSz":"Input Size",
    "iRc":"Input Records",
    "oSz":"Output Size",
    "oRc":"Output Records",
    "sFWTm":"Shuffle Read Blocked Time",
    "sLSz":"Shuffle Local Read Size",
    "sRDSz":"Shuffle Local Read To Disk Size",
    "sRRc":"Shuffle Read Records",
    "sRSz":"Shuffle Remote Read Size",
    "sWSz":"Shuffle Write Size",
    "sWRc":"Shuffle Write Records"
}
        
class MetricsAccumulator:
    def __init__(self):
        self.inputRecords = 0
        self.outputRecords = 0
        self.inputSize = 0
        self.outputSize = 0
        self.shuffleReadRecords = 0
        self.shuffleWriteRecords = 0
        self.shuffleRemoteReadSize = 0
        self.shuffleLocalReadSize = 0
        self.shuffleWriteSize = 0
        self.resultSize = 0
        self.bytesSpilledMemory = 0
        self.bytesSpilledDisk = 0
    def accumulate(self, dataAgg):
        self.inputRecords += dataAgg.inputRecords
        self.outputRecords += dataAgg.outputRecords
        self.inputSize += dataAgg.inputSize
        self.outputSize += dataAgg.outputSize
        self.shuffleReadRecords += dataAgg.shuffleReadRecords
        self.shuffleWriteRecords += dataAgg.shuffleWriteRecords
        self.shuffleRemoteReadSize += dataAgg.shuffleRemoteReadSize
        self.shuffleLocalReadSize += dataAgg.shuffleLocalReadSize
        self.shuffleWriteSize += dataAgg.shuffleWriteSize
        self.resultSize += dataAgg.resultSize
        self.bytesSpilledMemory += dataAgg.bytesSpilledMemory
        self.bytesSpilledDisk += dataAgg.bytesSpilledDisk
    def accumulateTaskMetrics(self, task):
        self.inputRecords += task.get('InputRecords', 0)
        self.outputRecords += task.get('OutputRecords', 0)
        self.inputSize += task.get('InputSize', 0)
        self.outputSize += task.get('OutputSize', 0)
        self.shuffleReadRecords += task.get('ShuffleTotalRecordsRead', 0)
        self.shuffleWriteRecords += task.get('ShuffleWriteRecords', 0)
        self.shuffleRemoteReadSize += task.get('ShuffleRemoteBytesRead', 0)
        self.shuffleLocalReadSize += task.get('ShuffleLocalBytesRead', 0)
        self.shuffleWriteSize += task.get('ShuffleWriteSize', 0)
        self.resultSize += task.get('ResultSize', 0)
        self.bytesSpilledMemory += task.get('MemoryBytesSpilled', 0)
        self.bytesSpilledDisk += task.get('DiskBytesSpilled', 0)
    def __str__(self):
        return str(self.__class__) + ": " + str(self.__dict__)

class TaskStats:
    def __init__(self):
        self.failedTasks = 0
        self.skippedTasks = 0
        self.succeededTasks = 0
        self.killedTasks = 0
        self.speculativeTasks = 0
        self.requiredTasks = 0
        self.retriedTasks = 0
        self.incompleteTasks = 0
    def accumulate(self, taskStats):
        self.failedTasks += taskStats.failedTasks
        self.skippedTasks += taskStats.skippedTasks
        self.succeededTasks += taskStats.succeededTasks
        self.killedTasks += taskStats.killedTasks
        self.speculativeTasks += taskStats.speculativeTasks
        self.requiredTasks += taskStats.requiredTasks
        self.retriedTasks += taskStats.retriedTasks
        self.incompleteTasks += taskStats.incompleteTasks
    def accumulateTaskStats(self, task):
        if 'Killed' in task and task['Killed']:
            self.killedTasks += 1
        if 'Speculative' in task and task['Speculative']:
            self.speculativeTasks += 1
        if 'Failed' in task and task['Failed']:
            self.failedTasks += 1
        if 'Speculative' in task and task['Speculative']:
            self.speculativeTasks += 1
        if 'EndReason' in task:
            if task['EndReason'] == 'Success':
                self.succeededTasks += 1
        else:
            self.incompleteTasks += 1
    def strStats(self):
        return '{}/{}{}{}{}'.format(self.succeededTasks,self.requiredTasks,
                    ' ({} failed)'.format(self.failedTasks) if self.failedTasks else '',
                    ' ({} killed)'.format(self.killedTasks) if self.killedTasks else '',
                    ' ({} skipped)'.format(self.skippedTasks) if self.skippedTasks else '')
    def __str__(self):
        return str(self.__class__) + ": " + str(self.__dict__)

class StageStats:
    def __init__(self):
        self.failedStages = 0
        self.skippedStages = 0
        self.succeededStages = 0
        self.killedStages = 0
        self.requiredStages = 0
        self.retriedStages = 0
    def accumulate(self, stageStats):
        self.failedStages += stageStats.failedStages
        self.skippedStages += stageStats.skippedStages
        self.succeededStages += stageStats.succeededStages
        self.killedStages += stageStats.killedStages
        self.requiredStages += stageStats.requiredStages
        self.retriedStages += stageStats.retriedStages
    def strStats(self):
        return '{}/{}{}{}'.format(self.succeededStages,self.requiredStages,
                    ' ({} failed)'.format(self.failedStages) if self.failedStages else '',
                    ' ({} skipped)'.format(self.skippedStages) if self.skippedStages else '')
    def __str__(self):
        return str({'fldS':self.failedStages,'skdS':self.skippedStages, 'sucS':self.succeededStages,
                    'kldS':self.killedStages,'reqS':self.requiredStages, 'rtdS':self.retriedStages})
        #return str(self.__class__) + ": " + str(self.__dict__)

class JobStats:
    def __init__(self):
        self.failedJobs = 0
        self.succeededJobs = 0
    def  accumulate(self, job):
        if 'JobResult' in job and job['JobResult'] == 'JobSucceeded':
            self.succeededJobs += 1
        else:
            self.failedJobs += 1
    def __str__(self):
        return str(self.__class__) + ": " + str(self.__dict__)

### This object will be saved to HTML, hence using small member names to reduce HTML size
class StageDetails:
    def __init__(self, jobId, stageAttempt, tasksStats, stagesStats, stageMetrics, stageDistributionMetrics, stageFailures):
        self.jobId = jobId
        self.stgId = stageAttempt.get('Id', None)
        self.attId = stageAttempt.get('StageAttemptId', None)
        self.stgN = stageAttempt.get('StageName', None)
        self.subTm = toDtStr(stageAttempt.get('SubmissionTime', None))
        ####self.comTm = toDtStr(stageAttempt.get('CompletionTime', None)) ### Removed
        ####self.durSt = toDurStr(stageAttempt.get('Duration', 0)) ### Removed
        self.durTm = stageAttempt.get('Duration', 0)
        self.inSz = stageMetrics.inputSize
        self.inRc = stageMetrics.inputRecords
        self.outSz = stageMetrics.outputSize
        self.outRc = stageMetrics.outputRecords
        self.sRSz = stageMetrics.shuffleRemoteReadSize + stageMetrics.shuffleLocalReadSize
        self.sRRc = stageMetrics.shuffleReadRecords
        self.sWSz = stageMetrics.shuffleWriteSize
        self.sWRc = stageMetrics.shuffleWriteRecords
        self.spDSz = stageMetrics.bytesSpilledDisk
        self.spMSz = stageMetrics.bytesSpilledMemory
        self.tskSt = tasksStats.strStats()
        self.tskCt = tasksStats.succeededTasks
        self.qntl = stageDistributionMetrics
        self.fLst = stageFailures
    def toJsonXXX(self):
        jsonData = self.__dict__
        jsonData['qntl'] = self.qntl.toJson()
        jsonData['fLst'] = [ x.toJson() for x in self.fLst ]
        return jsonData
    def toJson(self):
        return {
            "jsaIds": [ self.jobId, self.stgId, self.attId ],
            "stgN": self.stgN,
            "subTm": self.subTm,
            "durTm": self.durTm,
            "iSzRc": [ self.inSz, self.inRc ],
            "oSzRc": [ self.outSz, self.outRc ],
            "srSzRc": [ self.sRSz, self.sRRc ],
            "swSzRc": [ self.sWSz, self.sWRc ],
            "spDMSz": [ self.spDSz, self.spMSz ],
            "tsks": [ self.tskSt, self.tskCt ],
            "qntl": self.qntl.toJson(),
            "fLst": [ x.toJson() for x in self.fLst ]
        }

class StageFailures:
    def __init__(self,reason, count):
        self.reason = reason
        self.count = count
    def toJson(self):
        return self.__dict__
    def toList(self):
        return [
            self.reason,
            self.count
        ]

### This object will be saved to HTML, hence using small member names to reduce HTML size
class JobDetails:
    def __init__(self, job, tasksStats, stagesStats, jobMetrics):
        self.jobId = job.get('Id', None)
        self.grpId = job.get('JobGroupId', None)
        self.desc = job.get('JobDescription', None)
        self.stgN = job.get('FirstStageName', 'Unknown')
        self.subTm = toDtStr(job.get('SubmissionTime', None))
        ####self.comTm = toDtStr(job.get('CompletionTime', None)) ### Removed
        self.durTm = job.get('Duration', 0)
        self.stgSt = stagesStats.strStats()
        self.stgCt = stagesStats.succeededStages
        self.tskSt = tasksStats.strStats()
        self.tskCt = tasksStats.succeededTasks
        self.inSz = jobMetrics.inputSize
        self.outSz = jobMetrics.outputSize
        self.sRSz = jobMetrics.shuffleRemoteReadSize + jobMetrics.shuffleLocalReadSize
        self.sWSz = jobMetrics.shuffleWriteSize
        self.inRc = jobMetrics.inputRecords
        self.outRc = jobMetrics.outputRecords
        self.sRRc = jobMetrics.shuffleReadRecords
        self.sWRc = jobMetrics.shuffleWriteRecords
        self.spDSz = jobMetrics.bytesSpilledDisk
        self.spMSz = jobMetrics.bytesSpilledMemory
    def toJsonXXX(self):
        return self.__dict__
    def toJson(self):
        return {
            "jgIds": [ self.jobId, self.grpId ],
            "desc": [ self.desc, self.stgN ],
            "subTm": self.subTm,
            "durTm": self.durTm,
            "iSzRc": [ self.inSz, self.inRc ],
            "oSzRc": [ self.outSz, self.outRc ],
            "srSzRc": [ self.sRSz, self.sRRc ],
            "swSzRc": [ self.sWSz, self.sWRc ],
            "spDMSz": [ self.spDSz, self.spMSz ],
            "stgs": [ self.stgSt, self.stgCt ],
            "tsks": [ self.tskSt, self.tskCt ]
        }

filenameFormats = {
    'SparkListenerJobStart': '{}/job_start_{}.json',
    'SparkListenerJobEnd': '{}/job_end_{}.json',
    'SparkListenerStageSubmitted': '{}/stage_start_{}.json',
    'SparkListenerStageCompleted': '{}/stage_end_{}.json',
    'SparkListenerTaskStart': '{}/task_start_{}.json',
    'SparkListenerTaskEnd': '{}/task_end_{}.json'
}

def saveEventLog(eventData, outdir='', extra=''):
    full_data_outfile = './{}/eventlog{}.json'.format(outdir, extra)
    with open(full_data_outfile, 'w') as of:
        json_data = json.dumps(eventData, indent=4)
        of.write(json_data)

def createOutputDirectoryIfSpecified(outdir, dbJobId, dbRunId):
    if outdir:
        extractDir = '{}/_extractedJsonFiles/{}_{}/'.format(outdir, dbJobId, dbRunId)
        os.makedirs(extractDir, exist_ok=True)
        return extractDir
    
def dumpJson(event, filename, path):
    with open(os.path.join(path, filename), 'w') as of:
        of.write(json.dumps(event, indent=4))

def formatComma(input):
    return '{:,}'.format(input) if (isinstance(input, int) or isinstance(input, float)) else input

def dateFormat(input):
    return format(input, '%Y-%m-%d %H:%M:%S') if isinstance(input, datetime) else input

def toDT(dateInMS):
    if version == '3.x':
        return datetime.fromtimestamp(int(dateInMS/1000), timezone.utc) if dateInMS is not None else None
    else:
        return datetime.utcfromtimestamp(int(dateInMS/1000)) if dateInMS is not None else None

def toDtStr(dateInMS):
    try:
        if version == '3.x':
            return format(datetime.fromtimestamp(int(dateInMS/1000), timezone.utc), '%Y-%m-%d %H:%M:%S')
        else:
            return format(datetime.utcfromtimestamp(int(dateInMS/1000)), '%Y-%m-%d %H:%M:%S')
    except:
        return ''

def getDateDiffMs(fromDate, toDate):
    return (int(toDate) - int(fromDate))
    ##diff =  (datetime.fromtimestamp(int(toDate)/1000) - datetime.fromtimestamp(int(fromDate)/1000))
    ##return int(diff.total_seconds()*1000)

### Not using as it has linear interpolation for grouped data
def percentile(N, percent, key=lambda x:x):
    if not N:
        return None
    k = (len(N)-1) * percent
    f = math.floor(k)
    c = math.ceil(k)
    if f == c:
        return round(key(N[int(k)]), 2)
    d0 = key(N[int(f)]) * (c-k)
    d1 = key(N[int(c)]) * (k-f)
    return round(d0+d1, 2)
getMedian = functools.partial(percentile, percent=0.5)
getp25 = functools.partial(percentile, percent=0.25)
getp75 = functools.partial(percentile, percent=0.75)
getp95 = functools.partial(percentile, percent=0.95)

#def getPearsonModeSkewness(data):
#    data = [ x for x in data if x > 0 ]
#    if len(data) > 2:
#        mean = statistics.mean(data)
#        median = statistics.median_high(data)
#        sdev = statistics.stdev(data)
#        return abs(round((3 * (mean - median) / sdev), 2)) if sdev > 0 else 0
#    return 0
#
#def getSDev(data):
#    if len(data) > 2:
#        return 0 if not data else statistics.stdev(data)
#    return 0

def getSkew(data):
    data = [ x for x in data if x > 0 ]
    lenData = len(data)
    if lenData < 3:
        return 0
    mean = sum(data) / float(lenData)
    sumSquares, s3, s4 = 0, 0, 0
    for i in data:
        devScore = i - mean
        sumSquares += devScore ** 2
        s3 += devScore ** 3
        s4 += devScore ** 4
    variance = sumSquares / float(lenData-1)
    stdev = variance ** 0.5
    if stdev == 0:
        return 0
    return abs(int(s3 / (float(lenData-1) * stdev ** 3)))
    
#def getScipySkewness(data):
#    data = [ x for x in data if x > 0 ]
#    return abs(int(stats.skew(data), 2))

def getMean(data):
    return 0 if not data else int(sum(data) / float(len(data)))

def getMin(data):
    return 0 if not data else min(data)

def getMax(data):
    return 0 if not data else max(data)

def toKb(inBytes):
    return int(inBytes/1024)

def toMb(inBytes):
    return int(inBytes/1024/1024)

def toDurStr(ms):
    if ms < 1000:
        return '{:.0f} ms'.format(ms)
    min, sec = divmod(ms/1000, 60)
    hour, min = divmod(min, 60)
    return '{}{}{}'.format('{:.0f} h'.format(hour) if hour else '', ' {:.0f} m'.format(min) if  min else '',
                           ' {:.1f} s'.format(sec) if sec else '')

def zipLocalFilesAndCopyToTarget(files, targetDir = None):
    filename = 'jobReport.zip'
    tempZipfile = os.path.join('/tmp', filename)
    zipFs = zipfile.ZipFile(tempZipfile, 'w')
    for file in files:
        zipFs.write(file, os.path.basename(file), compress_type = zipfile.ZIP_DEFLATED)
    zipFs.close()
    files.append(tempZipfile)

    targetBasedir = '/dbfs/FileStore/'
    if os.path.isdir(targetBasedir):
        targetBasedir = os.path.join(targetBasedir, 'eventlogexplorerreports')
        os.makedirs(targetBasedir, exist_ok=True)
        targetPath = tempfile.mkdtemp(dir=targetBasedir)
    elif targetDir:
        targetPath = targetDir
    else:
        targetPath = os.getcwd()
    targetZipFile = os.path.join(targetPath, filename)
    copyfile(tempZipfile, targetZipFile)

    ## Clean up
    for file in files:
        if os.path.isfile(file):
            os.remove(file)

    ### Assuming this run is from the Databricks Notebooks
    if targetZipFile.startswith('/dbfs/FileStore/'):
        return 'files/' + targetZipFile[len('/dbfs/FileStore/'):]

    return targetZipFile
