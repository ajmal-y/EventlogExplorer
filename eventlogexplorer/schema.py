import os
import sys
import json
import math

taskSchema = {
    "SparkListenerTaskStart" : {
        "Stage ID": "StageId",
        "Stage Attempt ID" : "StageAttemptId",
        "Task Type": "TaskType",
        "Task Info": {
            "Task ID": "Id",
            "Index" : "Index",
            "Attempt" : "Attempt",
            "Launch Time": "LaunchTime",
            "Executor ID": "ExecutorId",
            "Host": "Host",
            "Locality": "Locality",
            "Speculative": "Speculative",
            "Getting Result Time": "GettingResultTime",
            "Finish Time": "FinishTime",
            "Failed": "Failed",
            "Killed": "Killed"
        }
    },
    "SparkListenerTaskEnd" : {
        "Stage ID": "StageId",
        "Stage Attempt ID" : "StageAttemptId",
        "Task Type": "TaskType",
        "Task Info": {
            "Task ID": "Id",
            "Index" : "Index",
            "Attempt" : "Attempt",
            "Launch Time": "LaunchTime",
            "Executor ID": "ExecutorId",
            "Host": "Host",
            "Locality": "Locality",
            "Speculative": "Speculative",
            "Getting Result Time": "GettingResultTime",
            "Finish Time": "FinishTime",
            "Failed": "Failed",
            "Killed": "Killed"
        },
        "Task End Reason": {
            "Reason": "EndReason",
            "Class Name": "ClassName",
            "Description": "Description"
        },
        "Task Metrics": {
            "Executor Deserialize Time": "ExecutorDeserializeTime",
            "Executor Run Time": "ExecutionDuration",
            "JVM GC Time": "GCTime",
            "Result Serialization Time": "ResultSerializationTime",
            "Memory Bytes Spilled": "MemoryBytesSpilled",
            "Disk Bytes Spilled": "DiskBytesSpilled",
            "Result Size": "ResultSize",
            "Shuffle Read Metrics": {
                "Remote Blocks Fetched": "ShuffleRemoteBlocksFetched",
                "Local Blocks Fetched": "ShuffleLocalBlocksFetched",
                "Fetch Wait Time": "ShuffleFetchWaitTime",
                "Remote Bytes Read": "ShuffleRemoteBytesRead",
                "Remote Bytes Read To Disk": "ShuffleRemoteBytesReadToDisk",
                "Local Bytes Read": "ShuffleLocalBytesRead",
                "Total Records Read": "ShuffleTotalRecordsRead"
            },
            "Shuffle Write Metrics": {
                "Shuffle Bytes Written": "ShuffleWriteSize",
                "Shuffle Write Time": "ShuffleWriteTime",  ### In Nanoseconds
                "Shuffle Records Written": "ShuffleWriteRecords"
            },
            "Input Metrics": {
                "Bytes Read": "InputSize",
                "Records Read": "InputRecords"
            },
            "Output Metrics": {
                "Bytes Written": "OutputSize",
                "Records Written": "OutputRecords"
            },
        }
    }
}

stageSchema = {
    "SparkListenerStageSubmitted" : {
        "Stage Info": {
            "Stage ID": "Id",
            "Stage Attempt ID" : "StageAttemptId",
            "Stage Name": "StageName",
            "Number of Tasks": "NumberOfTasks",
            "Details": "Details",
            "Submission Time": "SubmissionTime",
            "Parent IDs" : "ParentIDs",
            "---RDD Info": "RDDInfo"
        },
        "---Properties": "Properties"
    },
    "SparkListenerStageCompleted" : {
        "Stage Info": {
            "Stage ID": "Id",
            "Stage Attempt ID" : "StageAttemptId",
            "Stage Name": "StageName",
            "Number of Tasks": "NumberOfTasks",
            "Details": "Details",
            "Submission Time": "SubmissionTime",
            "Completion Time": "CompletionTime",
            "---Accumulables" : [
                "internal.metrics.output.recordsWritten",
                "internal.metrics.executorRunTime",
                "internal.metrics.jvmGCTime"
            ]
        }
    },
    "derivedSchema" : {
        "Properties": {
            "spark.jobGroup.id" : "JobGroupId",
            "spark.databricks.job.id" : "DatabricksJobId",
            "spark.databricks.job.runId" : "DatabricksRunId",
            "spark.job.description" : "StageDescription"
        }
    }
}

jobSchema = {
    "SparkListenerJobStart" : {
        "Job ID": "Id",
        "Submission Time": "SubmissionTime",
        "Stage IDs": "StageIds",
        "Parent IDs" : "ParentIDs",
        "---Stage Infos": "StageInfos",
        "Properties": "Properties"
    },
    "SparkListenerJobEnd" : {
        "Job ID": "Id",
        "Completion Time": "CompletionTime",
        "Job Result": {
            "Result": "JobResult"
        }
    },
    "derivedSchema" : {
        "Properties": {
            "spark.jobGroup.id" : "JobGroupId",
            "spark.databricks.job.id" : "DatabricksJobId",
            "spark.databricks.job.runId" : "DatabricksRunId",
            "spark.job.description" : "JobDescription"
        }
    }
}

sqlExecutionSchema = {
    "org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionStart" : {
        "executionId" : "Id",
        "description" : "Description",
        "physicalPlanDescription" : "Plan",
        "time" : "Time"
    }
}

