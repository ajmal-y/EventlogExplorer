import os
import re
import sys
import json
import tarfile
from .formatter import FormatterFactory

from .formatter import FormatterFactory
from .common import getDateDiffMs, dumpJson
from .schema import *

### ===========
# Should Support these filepath formats:
# https://logfood.cloud.databricks.com/files/logDump/spark-logs-v2/oregon-prod_0604-230732-cures695_20201024-002735_20201024-012735.tar.gz
# /dbfs/FileStore/logDump/spark-logs-v2/oregon-prod_0604-230732-cures695_20201024-002735_20201024-012735.tar.gz
# dbfs:/FileStorelogDump/spark-logs-v2/oregon-prod_0604-230732-cures695_20201024-002735_20201024-012735.tar.gz
# files/

### Spark prod_ds.spark_logs table fields
### spark_catalog.prod_ds.spark_logs.appId,
### spark_catalog.prod_ds.spark_logs.className,
### spark_catalog.prod_ds.spark_logs.clusterId,
### spark_catalog.prod_ds.spark_logs.date,
### spark_catalog.prod_ds.spark_logs.eventTime,
### spark_catalog.prod_ds.spark_logs.instanceId,
### spark_catalog.prod_ds.spark_logs.ip,
### spark_catalog.prod_ds.spark_logs.level,
### spark_catalog.prod_ds.spark_logs.logMessage,
### spark_catalog.prod_ds.spark_logs.logType,
### spark_catalog.prod_ds.spark_logs.metadata,
### spark_catalog.prod_ds.spark_logs.orgId,
### spark_catalog.prod_ds.spark_logs.publicHostName,
### spark_catalog.prod_ds.spark_logs.service,
### spark_catalog.prod_ds.spark_logs.shardName,
### spark_catalog.prod_ds.spark_logs.sparkImageLabel,
### spark_catalog.prod_ds.spark_logs.sparkVersion,
### spark_catalog.prod_ds.spark_logs.streamGUID,
### spark_catalog.prod_ds.spark_logs.streamOffset,
### spark_catalog.prod_ds.spark_logs.threadContext,
### spark_catalog.prod_ds.spark_logs.time,
### spark_catalog.prod_ds.spark_logs.uploadTime,
### spark_catalog.prod_ds.spark_logs.workerId,
### spark_catalog.prod_ds.spark_logs.workspaceId]

class Reader(object):
    def __init__(self, jobId=None, runId=None, isJobCluster=False):
        if not isJobCluster:
            if not (jobId and runId):
                raise ValueError("Reader: either 'isJobCluster' should be True OR 'jobId' and 'runId' should be provided")
        self.jobId = str(jobId) if jobId else None
        self.runId = str(runId) if runId else None
        self.isJobCluster = isJobCluster
        self.formatter = FormatterFactory()
    def read(self):
        pass
    def printDetails(self):
        if self.jobId:
            print('Job Id:', self.jobId)
        if self.runId:
            print('Run Id:', self.runId)
        print('Is Job Cluster:', self.isJobCluster)
    def _formatFilename(self, filename):
        if filename is not None:
            filename = filename.strip()
            if filename.startswith('dbfs:/'):
                filename = '/dbfs/' + filename[len('dbfs:/'):]
            elif filename.startswith('http'):
                filename = re.sub('^http[s]?://[^\/]*/files/', '/dbfs/FileStore/', filename)
        return filename
    def _getFormattedEvent(self, line):
        line = line.strip(' \n\t')
        if line != '':
            try:
                event = json.loads(line)
                #type, formattedEvent = self.formatter.getFormattedEvent(event)
                return self.formatter.getFormattedEvent(event)
            except Exception as ex:
                if line[0] != '{' and line[-1] != '}':
                    print('Ignoring line: {}'.format(line))
                else:
                    pass ### TODO
        return None, None

class FileReader(Reader):
    def __init__(self, eventLogfile, jobId=None, runId=None, isJobCluster=False, contentFilename=None):
        Reader.__init__(self, jobId, runId, isJobCluster)
        eventLogfile = self._formatFilename(eventLogfile)
        if tarfile.is_tarfile(eventLogfile):
            self.reader = TarFileReader(eventLogfile, jobId, runId, isJobCluster, contentFilename)
        else:
            self.reader = TextFileReader(eventLogfile, jobId, runId, isJobCluster)
    def printDetails(self):
        self.reader.printDetails()
    def read(self):
        return self.reader.read()

class TextFileReader(Reader):
    def __init__(self, eventLogfile, jobId=None, runId=None, isJobCluster=False):
        Reader.__init__(self, jobId, runId, isJobCluster)
        self.eventLogfile = self._formatFilename(eventLogfile)
    def printDetails(self):
        Reader.printDetails(self)
        if self.eventLogfile:
            print('Eventlog file:', self.eventLogfile)
    def read(self):
        print('Processing: {}'.format(self.eventLogfile))
        with open(self.eventLogfile, 'r') as file:
            for line in file:
                type, formattedEvent = self._getFormattedEvent(line)
                if formattedEvent is not None:
                    yield(type, formattedEvent)
        return

class TarFileReader(Reader):
    def __init__(self, tarFile, jobId=None, runId=None, isJobCluster=False, contentFilename=None):
        Reader.__init__(self, jobId, runId, isJobCluster)
        self.tarFile = self._formatFilename(tarFile)
        self.contentFilename = contentFilename.strip() if contentFilename else None
    def printDetails(self):
        Reader.printDetails(self)
        if self.tarFile:
            print('Tar file:', self.tarFile)
    def __isValid(self, file):
        if self.contentFilename:
            return True if file.name.endswith(self.contentFilename) else False
        if file.isreg() and ('/service=eventlog/' in file.name or file.name.endswith('eventlog.txt')):
            return True
        return False
    def read(self):
        contentFiles = []
        foundEventFile = False
        tfile = tarfile.open(self.tarFile, 'r|gz')
        for file in tfile:
            contentFiles.append(file)
            if self.__isValid(file):
                foundEventFile = True
                print('Processing: {}'.format(file.name))
                eventfile = tfile.extractfile(file)
                for line in eventfile:
                    type, formattedEvent = self._getFormattedEvent(line.decode())
                    if formattedEvent is not None:
                        yield(type, formattedEvent)
        if not foundEventFile:
            print('TarFileReader: Could not find a valid eventlog file in the archive! Skipping...')
            print("\tThe criteria is either the filename should be 'eventlog.txt' or the path should contain '/service=eventlog/'")
            print("\tPlease provide the filename (not full path) inside the archive as 'contentFilename' parameter")
            print('Tar Context:')
            for file in contentFiles:
                print(file)
        return

class SparkReader(Reader):
    def __init__(self, spark, table=None, clusterId=None, startTime=None, endTime=None, jobId=None, runId=None, isJobCluster=False):
        if jobId is None and runId is None:
            isJobCluster = True
        Reader.__init__(self, jobId, runId, isJobCluster)
        self.spark = spark
        self.table = table.strip() if table else 'spark_logs'
        if (startTime is None or endTime is None) and self.isPartitionedOnDate():
            raise ValueError("SparkReader: table, '{}' is partitioned on 'date'. Please provide startTime and endTime.".format(self.table))
        self.clusterId = clusterId.strip() if clusterId else None
        self.startTime = ' '.join(startTime.split()) if startTime else None
        self.endTime = ' '.join(endTime.split()) if endTime else None
    def isPartitionedOnDate(self):
        if '.' in self.table:
            database, table = self.table.split('.',1)
        else:
            database = None
        columns = self.spark.catalog.listColumns(table, database)
        for col in columns:
            if col.isPartition and col.name == 'date':
                return True
        return False
    def printDetails(self):
        Reader.printDetails(self)
        print('Spark object:', self.spark)
        print('Query:', self.__getQuery())
    def __dateWhereClause(self):
        where = ''
        if self.startTime and self.endTime:
            where += " AND eventTime >= '{}' AND eventTime <= '{}'".format(self.startTime, self.endTime)
            startDate = self.startTime.split(' ')[0]
            endDate = self.endTime.split(' ')[0]
            where += " AND date == '{}'".format(startDate) if startDate == endDate else " AND date >= '{}' AND date <= '{}'".format(startDate, endDate)
        elif self.startTime:
            where += " AND eventTime >= '{}'".format(self.startTime)
            where += " AND date == '{}'".format(self.startTime.split(' ')[0])
        elif self.endTime:
            where += " AND eventTime <= '{}'".format(self.endTime)
            where += " AND date == '{}'".format(self.endTime.split(' ')[0])
        if self.clusterId:
            where += " AND clusterId = '{}'".format(self.clusterId)
        return where

    def __getQuery(self):
        query = "SELECT logMessage FROM {}".format(self.table)
        query += " WHERE service = 'eventlog'"
        query += self.__dateWhereClause()
        query += " ORDER BY eventTime"
        return query

    def read(self):
        print("Fetching eventlogs from '{}'...".format(self.table))
        eventLogMessages = self.spark.sql(self.__getQuery()).coalesce(1).collect()
        for line in eventLogMessages:
            line = line.__getitem__("logMessage")
            type, formattedEvent = self._getFormattedEvent(line)
            if formattedEvent is not None:
                yield(type, formattedEvent)
        return


