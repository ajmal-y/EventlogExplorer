import os
import sys
import json
import argparse

from eventlogexplorer.jobBuilder import JobBuilder
from eventlogexplorer.reader import FileReader
from eventlogexplorer.common import saveEventLog

def main():
    discription = 'Generate Reports from Spark Event Logs'
    parser = argparse.ArgumentParser(description=discription)
    #parser._action_groups.pop()
    required_args = parser.add_argument_group('required arguments')
    parser.add_argument('-o', '--outputDir', help='Output directory', default='output')
    required_args.add_argument('-e', '--eventlog', help='Spark EventLog file/tar.gz file', dest='eventlog', required=True)
    parser.add_argument('-jr', '--jobId-runId', nargs = 2, help='JobId and RunId pair', dest='jobRun', metavar=('jobId','runId'))
    parser.add_argument('--saveas-json', help='Save all the Job details in a JSON file', dest='saveAsJson', action='store_true')
    parser.add_argument('-cf', '--content-filename', help="Name of the Eventlog json format file inside the tag.gz " +
                        "if its not the standard name as 'eventlog.txt'", dest='contentFilename')

    args = parser.parse_args()
    outdir = args.outputDir if args.outputDir else './output'
    (jobId, runId) = (args.jobRun[0], args.jobRun[1]) if args.jobRun else (None, None)
    isJobCluster = False if jobId else True
    print(jobId, runId, isJobCluster)

    if outdir[0] != '/':
        outdir = './' + outdir
    if not os.path.exists(outdir):
        os.makedirs(outdir)

    reader = FileReader(args.eventlog, jobId, runId, isJobCluster, args.contentFilename)
    reader.printDetails()
    databricksJob = JobBuilder(reader).getDatabricksJob().generateReport()
    if args.saveAsJson:
        saveEventLog(JobBuilder(reader).getDatabricksJobAsJson())

if __name__ == '__main__':
    main()


