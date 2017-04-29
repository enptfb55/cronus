
from croniter import croniter
import datetime
import time
from subprocess import call
import logging
import sys
import os
import sched
import argparse
import ConfigParser
from json_handle import JsonHandle
from mongodb_handle import MongoDBHandle

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger('cronus')

formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

file_handler = logging.FileHandler('cronus.log')
file_handler.setLevel(logging.DEBUG)
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)


def execute_job(name, cmd):
    ''' execute job
    '''

    logger.info('exec {0}: {1}'.format(name, cmd))
    call(cmd.split())


class Cronus(object):
    def __init__(self, handle):
        self.handle = handle
        self.sched = sched.scheduler(time.time, time.sleep)
    def schedule_job(self, job, run_time):
        ''' schedule job
        '''

        delay = (run_time - datetime.datetime.now()).total_seconds()
        logger.debug('exec {0} in {1}s'.format(job.get('name'), delay))
        self.sched.enter(delay, 1, execute_job, [job.get('name'), job.get('command')])

    def check_job(self, job):
        ''' check job 
        '''

        curr_time = datetime.datetime.now()
        curr_min = curr_time + datetime.timedelta(seconds = 60)

        if job.get('host') != os.uname()[1]:
            return

        start_time = job.get('start')
        if start_time:
            cron = croniter(start_time, curr_time)
            next_exec = cron.get_next(datetime.datetime)
            if next_exec <= curr_min:
                self.schedule_job(job, next_exec)

        end_time = job.get('end')
        if end_time:
            cron = croniter(end_time, curr_time)
            next_exec = cron.get_next(datetime.datetime)
            if next_exec <= curr_min:
                self.schedule_job(job, next_exec)


    def update(self):
        ''' update jobs 
        '''

        for job in self.handle.iter():
            if self.check_job(job):
                schedule_job(job)

    def run(self):
        ''' entry point 
        '''

        while True:
            self.update()
            self.sched.run()

def main():


    # create arg parser
    parser = argparse.ArgumentParser(description='Process some integers.')
    parser.add_argument('--config', dest='config', type=str,
                        help='Path to config')
    parser.add_argument('-f', '--file', dest='job_file', type=str,
                        help='Path to json job file')


    args = parser.parse_args()
    handle = None

    if args.job_file:
        handle = JsonHandle(args.job_file)

    if args.config:
        config = ConfigParser.ConfigParser()
        config.read(args.config)

        if config.get('Source', 'type') == 'mongodb':
            handle = MongoDBHandle(config.get('Source', 'mongo_host'),
                                   config.get('Source', 'mongo_port'),
                                   config.get('Source', 'mongo_db'),
                                   config.get('Source', 'mongo_collection'))

    cronus = Cronus(handle)
    cronus.run()

if __name__ == "__main__":
    main()
