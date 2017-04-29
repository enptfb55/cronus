
from croniter import croniter
import datetime
import time
from pymongo import MongoClient
from threading import Timer
from subprocess import call
import logging
import sys
import os
import sched
import argparse
import ConfigParser

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger('cronus')

formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

file_handler = logging.FileHandler('cronus.log')
file_handler.setLevel(logging.DEBUG)
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)


class Cronus(object):
    def __init__(self, handle):
        self.handle = handle
        self.sched = sched.scheduler(time.time, time.sleep)

    def exec_job(self, name, cmd):
        logger.info('exec {0}: {1}'.format(name, cmd))
        call(cmd.split())

    def schedule_job(self, job, run_time):
        delay = (run_time - datetime.datetime.now()).total_seconds()
        logger.debug('exec {0} in {1}s'.format(job.get('name'), delay))
        self.sched.enter(delay, 1, exec_job, [self, job.get('name'), job.get('command')])

    def check_job(self, job):
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
        ''' '''
        for job in self.handle.find({}):
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
    parser.add_argument('--config', dest='config', type=str, default='config.ini',
                        help='Path to config')

    args = parser.parse_args()
    handle = None

    if args.config:
        config = ConfigParser.ConfigParser()
        config.read(args.config)

        if config.get('Source', 'type') == 'mongodb':
            client = MongoClient(config.get('Source', 'mongo_host'),
                                 int(config.get('Source', 'mongo_port')))

            handle = client[config.get('Source', 'mongo_db')][config.get('Source', 'mongo_collection')]

    cronus = Cronus(handle)
    cronus.run()

if __name__ == "__main__":
    main()
