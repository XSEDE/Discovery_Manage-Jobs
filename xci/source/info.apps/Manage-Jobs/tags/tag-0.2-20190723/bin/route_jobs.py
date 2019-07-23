#!/usr/bin/env python

# Expand ComputingQueue Model contents
#   into the ComputingActivity Model
# from a source (Model, amqp, file, directory)
#   to a destination (print, directory, warehouse, api)

import amqp
import argparse
import base64
import json
import logging
import logging.handlers
import os
import pwd
import re
import shutil
import signal
import socket
import ssl
from ssl import _create_unverified_context
import sys
from time import sleep

try:
    import http.client as httplib
except ImportError:
    import httplib

import django
django.setup()
from django.db import DataError, IntegrityError
from django.conf import settings
from rest_framework import status
from glue2_provider.process import Glue2ProcessRawIPF, StatsSummary
from glue2_db.models import ComputingActivity, ComputingQueue
from xsede_warehouse.exceptions import ProcessingException
from xsede_warehouse.stats import StatsTracker

# Use datetime for local and timezone non-sensitive data
# Use timezone for distributed and timezone sensitive data
import datetime
from datetime import datetime, timedelta
from django.utils import timezone
from django.utils.dateparse import parse_datetime

from daemon import runner
import pdb

# Activity cache to avoid unnecessary db updates and enhance performance
# Uses a fingerprint of relevant fields
# New activities that match the cache aren't updated
# The cache has a timestamp we can use to expire and reset the contents
a_cache = {}
a_cache_ts = timezone.now()

#
# Time capture sequence:
#   Job.CreationTime: when the job entry was captured AT THE SP (origin)
#   QueueReceivedTS:  when the job queue was received by the glue2 router
#                     to store in the ComputingQueue model
#   ProcessStartTS:   when the job queue processing started by the job router
#   timezone.now():   when the job queue processing finished (when we're called)
#
# Timings collected:
#    recv_*         elapsed from job capture at SP to received by glue2 router
#                   (average for all jobs in a queue which should be identical)
#    pstart_*       elapsed from job received by glue2 router to processing started in jobs router
#                   (average for queues)
#    pdone_*        timing from processing started in jobs router to processing done
#                   (average for queues)
#
# Performance metrics for each resource
# -> [ResourceID] -> 'queue_count' = <total job queues was processed>
#                 -> 'jobs_count' = <total jobs across all job queues processed>
#                 -> 'totaltime_recv' = <total time from query at SP(Job.CreationTime)
#                                   to route_glue2 received (ComputingQueue.CreationTime)>
#                 -> 'totaltime_pstart' = <total time from route_glue2 received
#                                   to processing started
#                 -> 'totaltime_pdone' = <total time from processing started to done
#
PerfMet = {}
PerfMet_count = 0
PerfMet_enabled = False   # When we start procesing recent queues

def capture_metrics(ResourceID, Jobs, QueueReceivedTS, ProcessStartTS):
    global PerfMet_enabled
    if not PerfMet_enabled:
        return
    recv_ttime = 0
    recv_count = 0
    first_capture = None
    for j in Jobs:
        try:
            capture_time = parse_datetime(Jobs[j]['CreationTime']) + timedelta(seconds=0.5) # More accurage
        except:
            pass
        else:
            if capture_time: # Skip if None
                if first_capture is None:
                    first_capture = capture_time
                else:
                    first_capture = min(first_capture, capture_time)
                recv_count += 1
                recv_ttime += (QueueReceivedTS - capture_time).total_seconds()

#    print('{}: {} {} {}'.format(ResourceID, first_capture, QueueReceivedTS, ProcessStartTS))

    global PerfMet
    if ResourceID not in PerfMet:
        PerfMet[ResourceID] = {'queue_count': 0, 'jobs_count': 0, 'totaltime_recv': 0, 'totaltime_pstart': 0, 'totaltime_proc': 0}
    my_pm = PerfMet[ResourceID]
    my_pm['queue_count'] += 1
    my_pm['jobs_count'] += recv_count
    if recv_count > 0:
        my_pm['totaltime_recv'] += recv_ttime / recv_count   # The average for jobs in a queue
    my_pm['totaltime_pstart'] += (ProcessStartTS - QueueReceivedTS).total_seconds()
    my_pm['totaltime_proc'] += (timezone.now() - ProcessStartTS).total_seconds()
    global PerfMet_count
    PerfMet_count += 1

def dumPerfMet(self):
    global PerfMet_count
    if PerfMet_count <= 100:
        return
    global PerfMet
    t_count = 0
    t_items = 0
    t_recv = 0
    t_pstart = 0
    t_proc = 0
    for ResourceID in PerfMet:
        my_pm = PerfMet[ResourceID]
        if my_pm['queue_count'] > 0:
            avg_jobs = my_pm['jobs_count'] / my_pm['queue_count']
            avg_recv = my_pm['totaltime_recv'] / my_pm['queue_count']
            avg_pstart = my_pm['totaltime_pstart'] / my_pm['queue_count']
            avg_proc = my_pm['totaltime_proc'] / my_pm['queue_count']
            self.logger.debug('METRICS resourceid={} {}/items: jobs/item={}, to_receive={}, to_beginprocess={}, to_process={}'.format(ResourceID, my_pm['queue_count'], avg_jobs, avg_recv, avg_pstart, avg_proc))
            t_count += 1
            t_items += my_pm['queue_count']
            t_recv += avg_recv
            t_pstart += avg_pstart
            t_proc += avg_proc

    self.logger.info('METRICS AVERAGE {}/items: to_receive={}, to_beginprocess={}, to_process={}, end-to-end={}'.format(t_items, t_recv / t_count, t_pstart / t_count, t_proc / t_count, (t_recv / t_count) + (t_pstart / t_count) + (t_proc / t_count)))
    PerfMet = {}
    PerfMet_count = 0

def get_Validity(obj):
    try:
        val = timedelta(seconds=obj['Validity'])
    except:
        val = None
    return val

class Route_Jobs():
    def __init__(self):
        self.args = None
        self.config = {}
        self.src = {}
        self.dest = {}
        for var in ['type', 'obj', 'host', 'port', 'display']:
            self.src[var] = None
            self.dest[var] = None

        parser = argparse.ArgumentParser(epilog='File|Directory SRC|DEST syntax: {file|directory}:<file|directory path and name')
        parser.add_argument('daemonaction', nargs='?', choices=('start', 'stop', 'restart'), \
                            help='{start, stop, restart} daemon')
        parser.add_argument('-s', '--source', action='store', dest='src', \
                            help='Messages source {amqp, file, directory} (default=amqp)')
        parser.add_argument('-d', '--destination', action='store', dest='dest', \
                            help='Message destination {print, directory, warehouse, or api} (default=print)')
        parser.add_argument('-l', '--log', action='store', \
                            help='Logging level (default=warning)')
        parser.add_argument('-c', '--config', action='store', default='./route_jobs.conf', \
                            help='Configuration file default=./route_jobs.conf')
        # Don't set the default so that we can apply the precedence argument || config || default
        parser.add_argument('-q', '--queue', action='store', \
                            help='AMQP queue default=jobs-router')
        parser.add_argument('--verbose', action='store_true', \
                            help='Verbose output')
        parser.add_argument('--daemon', action='store_true', \
                            help='Daemonize execution')
        parser.add_argument('--pdb', action='store_true', \
                            help='Run with Python debugger')
        self.args = parser.parse_args()

        if self.args.pdb:
            pdb.set_trace()

        # Load configuration file
        self.config_file = os.path.abspath(self.args.config)
        try:
            with open(self.config_file, 'r') as file:
                conf=file.read()
                file.close()
        except IOError as e:
            raise
        try:
            self.config = json.loads(conf)
        except ValueError as e:
            self.logger.error('Error "{}" parsing config={}'.format(e, self.config_file))
            sys.exit(1)

        # Initialize logging
        numeric_log = None
        if self.args.log is not None:
            numeric_log = getattr(logging, self.args.log.upper(), None)
        if numeric_log is None and 'LOG_LEVEL' in self.config:
            numeric_log = getattr(logging, self.config['LOG_LEVEL'].upper(), None)
        if numeric_log is None:
            numeric_log = getattr(logging, 'INFO', None)
        if not isinstance(numeric_log, int):
            raise ValueError('Invalid log level: {}'.format(numeric_log))
        self.logger = logging.getLogger('xsede.glue2')
        self.logger.setLevel(numeric_log)

        # Verify arguments and parse compound arguments
        if 'src' not in self.args or not self.args.src: # Tests for None and empty ''
            if 'SOURCE' in self.config:
                self.args.src = self.config['SOURCE']
        if 'src' not in self.args or not self.args.src:
            self.args.src = 'amqp:infopub.xsede.org:5671'
        idx = self.args.src.find(':')
        if idx > 0:
            (self.src['type'], self.src['obj']) = (self.args.src[0:idx], self.args.src[idx+1:])
        else:
            self.src['type'] = self.args.src
        if self.src['type'] == 'dir':
            self.src['type'] = 'directory'
        elif self.src['type'] not in ['amqp', 'file', 'directory', 'queuetable']:
            self.logger.error('Source not {amqp, file, directory}')
            sys.exit(1)
        if self.src['type'] == 'amqp':
            idx = self.src['obj'].find(':')
            if idx > 0:
                (self.src['host'], self.src['port']) = (self.src['obj'][0:idx], self.src['obj'][idx+1:])
            else:
                self.src['host'] = self.src['obj']
            if not self.src['port']:
                self.src['port'] = '5671'
            self.src['display'] = '{}@{}:{}'.format(self.src['type'], self.src['host'], self.src['port'])
        elif self.src['type'] == 'queuetable':
            self.src['display'] = '{}@database={}'.format(self.src['type'], settings.DATABASES['default']['HOST'])
        elif self.src['obj']:
            self.src['display'] = '{}:{}'.format(self.src['type'], self.src['obj'])
        else:
            self.src['display'] = self.src['type']

        if 'dest' not in self.args or not self.args.dest:
            if 'DESTINATION' in self.config:
                self.args.dest = self.config['DESTINATION']
        if 'dest' not in self.args or not self.args.dest:
            self.args.dest = 'print'
        idx = self.args.dest.find(':')
        if idx > 0:
            (self.dest['type'], self.dest['obj']) = (self.args.dest[0:idx], self.args.dest[idx+1:])
        else:
            self.dest['type'] = self.args.dest
        if self.dest['type'] == 'dir':
            self.dest['type'] = 'directory'
        elif self.dest['type'] not in ['print', 'directory', 'warehouse', 'api']:
            self.logger.error('Destination not {print, directory, warehouse, api}')
            sys.exit(1)
        if self.dest['type'] == 'api':
            idx = self.dest['obj'].find(':')
            if idx > 0:
                (self.dest['host'], self.dest['port']) = (self.dest['obj'][0:idx], self.dest['obj'][idx+1:])
            else:
                self.dest['host'] = self.dest['obj']
            if not self.dest['port']:
                self.dest['port'] = '443'
            self.dest['display'] = '{}@{}:{}'.format(self.dest['type'], self.dest['host'], self.dest['port'])
        elif self.dest['type'] == 'warehouse':
            self.dest['display'] = '{}@database={}'.format(self.dest['type'], settings.DATABASES['default']['HOST'])
        elif self.dest['obj']:
            self.dest['display'] = '{}:{}'.format(self.dest['type'], self.dest['obj'])
        else:
            self.dest['display'] = self.dest['type']

        if self.src['type'] in ['file', 'directory'] and self.dest['type'] == 'directory':
            self.logger.error('Source {file, directory} can not be routed to Destination {directory}')
            sys.exit(1)

        if self.dest['type'] == 'directory':
            if not self.dest['obj']:
                self.dest['obj'] = os.getcwd()
            self.dest['obj'] = os.path.abspath(self.dest['obj'])
            if not os.access(self.dest['obj'], os.W_OK):
                self.logger.error('Destination directory={} not writable'.format(self.dest['obj']))
                sys.exit(1)
        if self.args.daemonaction:
            self.stdin_path = '/dev/null'
            if 'LOG_FILE' in self.config:
                self.stdout_path = self.config['LOG_FILE'].replace('.log', '.daemon.log')
                self.stderr_path = self.stdout_path
            else:
                self.stdout_path = '/dev/tty'
                self.stderr_path = '/dev/tty'
            self.SaveDaemonLog(self.stdout_path)
            self.pidfile_timeout = 5
            if 'PID_FILE' in self.config:
                self.pidfile_path =  self.config['PID_FILE']
            else:
                name = os.path.basename(__file__).replace('.py', '')
                self.pidfile_path =  '/var/run/{}/{}.pid'.format(name ,name)

    def SaveDaemonLog(self, path):
        # Save daemon log file using timestamp only if it has anything unexpected in it
        try:
            with open(path, 'r') as file:
                lines=file.read()
                file.close()
                if not re.match("^started with pid \d+$", lines) and not re.match("^$", lines):
                    ts = datetime.strftime(timezone.now(), '%Y-%m-%d_%H:%M:%S')
                    newpath = '{}.{}'.format(path, ts)
                    shutil.copy(path, newpath)
                    print('SaveDaemonLog as ' + newpath)
        except Exception as e:
            print('Exception in SaveDaemonLog({})'.format(path))
        return

    def exit_signal(self, signal, frame):
        self.logger.error('Caught signal, exiting...')
        sys.exit(0)
    
    def ConnectAmqp_Anonymous(self):
        conn = amqp.Connection(host='{}:{}'.format(self.src['host'], self.src['port']), virtual_host='xsede')
    #                           heartbeat=2)
        conn.connect()
        return conn

    def ConnectAmqp_UserPass(self):
        ssl_opts = {'ca_certs': os.environ.get('X509_USER_CERT')}
        conn = amqp.Connection(host='{}:{}'.format(self.src['host'], self.src['port']), virtual_host='xsede',
                               userid=self.config['AMQP_USERID'], password=self.config['AMQP_PASSWORD'],
                               heartbeat=60,
                               ssl=ssl_opts)
        conn.connect()
        return conn

    def ConnectAmqp_X509(self):
        ssl_opts = {'ca_certs': self.config['X509_CACERTS'],
                   'keyfile': '/path/to/key.pem',
                   'certfile': '/path/to/cert.pem'}
        conn = amqp.Connection(host='{}:{}'.format(self.src['host'], self.src['port']), virtual_host='xsede',
                               heartbeat=60,
                               ssl=ssl_opts)
        conn.connect()
        return conn

    def dest_print(self, st, doctype, resourceid, message_body):
        print('{} exchange={}, routing_key={}, size={}, dest=PRINT'.format(st, doctype, resourceid, len(message_body) ) )
        if self.dest['obj'] != 'dump':
            return
        try:
            py_data = json.loads(message_body)
        except ValueError as e:
            self.logger.error('Parsing Exception: {}'.format(e))
            return
        for key in py_data:
            print('  Key=' + key)

    def dest_directory(self, st, doctype, resourceid, message_body):
        dir = os.path.join(self.dest['obj'], doctype)
        if not os.access(dir, os.W_OK):
            self.logger.critical('{} exchange={}, routing_key={}, size={} Directory not writable "{}"'.format(st, doctype, resourceid, len(message_body), dir ) )
            return
        file_name = resourceid + '.' + st
        file = os.path.join(dir, file_name)
        self.logger.info('{} exchange={}, routing_key={}, size={} dest=file:<exchange>/{}'.format(st, doctype, resourceid, len(message_body), file_name ) )
        with open(file, 'w') as fd:
            fd.write(message_body)
            fd.close()

    def dest_restapi(self, st, doctype, resourceid, message_body):
        if doctype in ['glue2.computing_activity']:
            self.logger.info('exchange={}, routing_key={}, size={} dest=DROP'.format(doctype, resourceid, len(message_body) ) )
            return

        headers = {'Content-type': 'application/json',
            'Authorization': 'Basic {}'.format(base64.standard_b64encode( (self.config['API_USERID'] + ':' + self.config['API_PASSWORD']).encode() )).decode() }
        url = '/glue2-provider-api/v1/process/doctype/{}/resourceid/{}/'.format(doctype, resourceid)
        if self.dest['host'] not in ['localhost', '127.0.0.1'] and self.dest['port'] != '8000':
            url = '/wh1' + url
#        (host, port) = (self.dest['host'].encode('utf-8'), self.dest['port'].encode('utf-8'))
        (host, port) = (self.dest['host'], self.dest['port'])
        retries = 0
        while retries < 100:
            try:
                if self.dest['port'] == '443':
    #                ssl_con = ssl.create_default_context(purpose=ssl.Purpose.SERVER_AUTH, capath='/etc/grid-security/certificates/')
    #                ssl_con.load_default_certs()
    #                ssl_con.load_cert_chain('certkey.pem')
                    ssl_con = ssl._create_unverified_context(check_hostname=False, \
                                                             certfile=self.config['X509_CERT'], keyfile=self.config['X509_KEY'])
                    conn = httplib.HTTPSConnection(host, port, context=ssl_con)
                else:
                    conn = httplib.HTTPConnection(host, port)
                self.logger.debug('POST {}'.format(url))
                conn.request('POST', url, message_body, headers)
                response = conn.getresponse()
                self.logger.info('RESP exchange={}, routing_key={}, size={} dest=POST http_response=status({})/reason({})'.format(doctype, resourceid, len(message_body), response.status, response.reason ) )
                data = response.read()
                conn.close()
                break
            except (socket.error) as e:
                retries += 1
                sleepminutes = 2*retries
                self.logger.error('Exception socket.error to {}:{}; sleeping {}/minutes before retrying'.format(host, port, sleepminutes))
                sleep(sleepminutes*60)
            except (httplib.BadStatusLine) as e:
                retries += 1
                sleepminutes = 2*retries
                self.logger.error('Exception httplib.BadStatusLine to {}:{}; sleeping {}/minutes before retrying'.format(host, port, sleepminutes))
                sleep(sleepminutes*60)

        if response.status in [400, 403]:
            self.logger.error('response={}'.format(data))
            return
        try:
            obj = json.loads(data)
        except ValueError as e:
            self.logger.error('API response not in expected format ({})'.format(e))

    def dest_warehouse(self, ts, doctype, resourceid, message_body):
        proc = Glue2ProcessRawIPF(application=os.path.basename(__file__), function='dest_warehouse')
        (code, message) = proc.process(ts, doctype, resourceid, message_body)

    def process_file(self, path):
        file_name = path.split('/')[-1]
        if file_name[0] == '.':
            return
        
        idx = file_name.rfind('.')
        resourceid = file_name[0:idx]
        ts = file_name[idx+1:len(file_name)]
        with open(path, 'r') as file:
            data=file.read().replace('\n','')
            file.close()
        try:
            py_data = json.loads(data)
        except ValueError as e:
            self.logger.error('Parsing "{}" Exception: {}'.format(path, e))
            return

        if 'ApplicationEnvironment' in py_data or 'ApplicationHandle' in py_data:
            doctype = 'glue2.applications'
        elif 'ComputingManager' in py_data or 'ComputingService' in py_data or \
            'ExecutionEnvironment' in py_data or 'Location' in py_data or 'ComputingShare' in py_data:
            doctype = 'glue2.compute'
        elif 'ComputingActivity' in py_data:
            doctype = 'glue2.computing_activities'
        else:
            self.logger.error('Document type not recognized: ' + path)
            return
        self.logger.info('Processing file: ' + path)

        if self.dest['type'] == 'api':
            self.dest_restapi(ts, doctype, resourceid, data)
        elif self.dest['type'] == 'print':
            self.dest_print(ts, doctype, resourceid, data)

    def process_queuetable(self):
        # Frequency to load ComputingQueue (our input)
        Queue_RefreshInterval = timedelta(seconds=5)
        Queue_RefreshTS = timezone.now() - Queue_RefreshInterval # In the past to force initial refresh
        Queue_In = {}
        Queue_Done = {}

        while True:
            ProcessStartTS = timezone.now()                  # Processing start timestamp
            Iter_Processed = 0                               # How many resource queues we processed
            Iter_Sleep = 0                                   # Default no sleep at end of iteration
            
            if ProcessStartTS > Queue_RefreshTS:             # Refresh the input queue if past the refresh TS
                Queue_In = {}
                objects = ComputingQueue.objects.all()
                for obj in objects:
                    sortkey = '{:%Y-%m-%d %H:%M:%S.%f}:{}'.format(obj.CreationTime, obj.ResourceID)
                    Queue_In[sortkey] = obj
                Queue_RefreshTS = timezone.now() + Queue_RefreshInterval  # Next refresh
        
            for key in sorted(Queue_In):
                ResourceID = Queue_In[key].ResourceID
                CreationTime = Queue_In[key].CreationTime
                if Queue_Done.get(ResourceID) == CreationTime:
                    continue # No change since last processed
                Jobs = Queue_In[key].EntityJSON
                self.process_jobs(ResourceID, CreationTime, Jobs)
                capture_metrics(ResourceID, Jobs, CreationTime, ProcessStartTS)
                Queue_Done[ResourceID] = CreationTime
                Iter_Processed += 1
                if timezone.now() > Queue_RefreshTS:       # Break out to refresh the queue
                    break
            else: # We finished the sorted(Queue_In)
                Iter_Sleep = max(0.01, (Queue_RefreshTS - timezone.now()).total_seconds()) # At least 1/100 sec.

            Iter_FinishTS = timezone.now()
            Iter_Seconds = (Iter_FinishTS - ProcessStartTS).total_seconds()
            if Iter_Sleep == 0:
                self.logger.info('Iteration {}/queues in {:0.6f}/sec'.format(Iter_Processed, Iter_Seconds ))
            else:
                if Iter_Processed > 0:
                    self.logger.info('Iteration {}/queues in {:0.6f}/sec, sleeping={:0.6f}/sec'.format(Iter_Processed, Iter_Seconds, Iter_Sleep))
                dumPerfMet(self)
                try:
                    sleep(Iter_Sleep)
                    global PerfMet_enabled
                    PerfMet_enabled = True
                except KeyboardInterrupt:
                    raise
                except Exception:
                    self.logger.error('Failed sleep({})'.format(Iter_Sleep))

    def process_jobs(self, ResourceID, CreationTime, Jobs):
        ########################################################################
        # Stores individual job entries
        # Improve ComputingActivity awareness so as to only write what needs to be written
        ########################################################################
        me = 'ComputingActivity'
        # Load new entries
        self.resourceid = ResourceID
        self.stats = StatsTracker(self.resourceid, me)
        self.new = {}   # Contains new object json
        self.cur = {}   # Contains existing object references
        self.new[me] = {}
        self.cur[me] = {}

# Filter out 'Extensions' and other non-jobs
#        self.new[me] = Jobs
        for k in Jobs:
            if k.startswith('urn:glue2:ComputingActivity:'):
                self.new[me][k] = Jobs[k]
        self.stats.set('{}.New'.format(me), len(self.new[me]))
        self.logger.debug('Processing {} {}/jobs'.format(ResourceID, len(self.new[me])))

        # Load current database entries
        if not self.cur[me]:
            for item in ComputingActivity.objects.filter(ResourceID=self.resourceid):
                self.cur[me][item.ID] = item
            self.stats.set('{}.Current'.format(me), len(self.cur[me]))

        # Add/update entries
        for ID in self.new[me]:
            if ID in self.cur[me] and parse_datetime(self.new[me][ID]['CreationTime']) <= self.cur[me][ID].CreationTime:
#               self.new[me][ID]['model'] = self.cur[me][ID]    # Save the latest object reference
                continue                                        # Don't update database since is has the latest

            if self.activity_is_cached(ID, self.new[me][ID]):
                self.stats.add('{}.ToCache'.format(me), 1)
                continue

            new_name = self.new[me][ID].get('Name', 'none')
            try:
                new_localowner = self.new[me][ID].get('LocalOwner', 'unknown')
            except:
                new_localowner = 'unknown'
            
            if new_name is not None:
                new_name = new_name[:128]
            try:
                model = ComputingActivity(ID=self.new[me][ID]['ID'],
                                          ResourceID=self.resourceid,
                                          Name=new_name,
                                          CreationTime=self.new[me][ID]['CreationTime'],
                                          Validity=get_Validity(self.new[me][ID]),
                                          LocalOwner=new_localowner,
                                          EntityJSON=self.new[me][ID])
                model.save()
#               self.new[me][ID]['model'] = model
                self.stats.add('{}.Updates'.format(me), 1)

                self.activity_to_cache(ID, self.new[me][ID])
            
            except (DataError, IntegrityError, TypeError) as e:
                raise ProcessingException('{} updating {} (ID={}): {}'.format(type(e).__name__, me, self.new[me][ID]['ID'], \
                                        e.message), status=status.HTTP_400_BAD_REQUEST)

        # Delete old entries
        dels = []
        for ID in self.cur[me]:
            if ID in self.new[me]:
                continue
            try:
                ComputingActivity.objects.filter(ID=ID).delete()
                dels.append(ID)
                self.stats.add('{}.Deletes'.format(me), 1)
            except (DataError, IntegrityError) as e:
                raise ProcessingException('{} deleting {} (ID={}): {}'.format(type(e).__name__, me, ID, e.message), \
                                          status=status.HTTP_400_BAD_REQUEST)
        for ID in dels:
            self.cur[me].pop(ID)

        self.stats.end()
        self.logger.info(self.stats.summary())

    def activity_is_cached(self, id, obj): # id=object unique id
        global a_cache   	# JobID is key, fingerprint fields are the value
        global a_cache_ts
        if (timezone.now() - a_cache_ts).total_seconds() > 3600:    # Expire cache every hour (3600 seconds)
            a_cache = {}
            a_cache_ts = timezone.now()
            self.logger.debug('Expiring Activity cache')

        return id in a_cache and a_cache[id] == self.activity_hash(obj)

    def activity_to_cache(self, id, obj):
        global a_cache
        a_cache[id] = self.activity_hash(obj)

    def activity_hash(self, obj):
	# These fields are the fingerprint for what is in the database
        hash_list = []
        if 'State' in obj:
            hash_list.append(obj['State'])
        if 'UsedTotalWallTime' in obj:
            hash_list.append(obj['UsedTotalWallTime'])
        return(json.dumps(hash_list))

    def amqp_consume_setup(self):
        now = datetime.utcnow()
        try:
            if (now - self.amqp_consume_setup_last).seconds < 300:  # 5 minutes
                self.logger.error('Too recent amqp_consume_setup, quitting...')
                sys.exit(1)
        except SystemExit:
            raise
        except:
            pass
        self.amqp_consume_setup_last = now

        self.conn = self.ConnectAmqp_UserPass()
        self.channel = self.conn.channel()
        self.channel.basic_qos(prefetch_size=0, prefetch_count=4, a_global=True)
        which_queue = self.args.queue or self.config.get('QUEUE', 'jobs-router')
        queue = self.channel.queue_declare(queue=which_queue, durable=True, auto_delete=False).queue
        exchanges = ['glue2.computing_activities']
        for ex in exchanges:
            self.channel.queue_bind(queue, ex, '#')
        self.logger.info('AMQP Queue={}, Exchanges=({})'.format(which_queue, ', '.join(exchanges)))
        st = datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ')
        self.channel.basic_consume(queue, callback=self.amqp_callback)


###############################################################################################
# Where we process
###############################################################################################
    def run(self):
        signal.signal(signal.SIGINT, self.exit_signal)

        self.logger.info('Starting program={} pid={} uid={}({})'.format(os.path.basename(__file__), os.getpid(), os.geteuid(), pwd.getpwuid(os.geteuid()).pw_name))
        self.logger.info('Source: ' + self.src['display'])
        self.logger.info('Destination: ' + self.dest['display'])

        if self.src['type'] == 'amqp':
            self.amqp_consume_setup()
            while True:
                try:
                    self.conn.drain_events()
                    self.conn.heartbeat_tick(rate=2)
                    continue # Loops back to the while
                except (socket.timeout):
                    self.logger.info('AMQP drain_events timeout, sending heartbeat')
                    self.conn.heartbeat_tick(rate=2)
                    sleep(5)
                    continue
                except Exception as err:
                    self.logger.error('AMQP drain_events error: ' + format(err))
                try:
                    self.conn.close()
                except Exception as err:
                    self.logger.error('AMQP connection.close error: ' + format(err))
                sleep(30)   # Sleep a little and then try to reconnect
                self.amqp_consume_setup()

        elif self.src['type'] == 'file':
            self.src['obj'] = os.path.abspath(self.src['obj'])
            if not os.path.isfile(self.src['obj']):
                self.logger.error('Source is not a readable file={}'.format(self.src['obj']))
                sys.exit(1)
            self.process_file(self.src['obj'])

        elif self.src['type'] == 'directory':
            self.src['obj'] = os.path.abspath(self.src['obj'])
            if not os.path.isdir(self.src['obj']):
                self.logger.error('Source is not a readable directory={}'.format(self.src['obj']))
                sys.exit(1)
            for file1 in os.listdir(self.src['obj']):
                fullfile1 = os.path.join(self.src['obj'], file1)
                if os.path.isfile(fullfile1):
                    self.process_file(fullfile1)
                elif os.path.isdir(fullfile1):
                    for file2 in os.listdir(fullfile1):
                        fullfile2 = os.path.join(fullfile1, file2)
                        if os.path.isfile(fullfile2):
                            self.process_file(fullfile2)

        elif self.src['type'] == 'queuetable':
            self.process_queuetable()

if __name__ == '__main__':
    router = Route_Jobs()
    if router.args.daemonaction is None:
        # Interactive execution
        myrouter = router.run()
        sys.exit(0)

# Daemon execution
    daemon_runner = runner.DaemonRunner(router)
    daemon_runner.daemon_context.files_preserve=[router.logger.handlers[0].stream]
    daemon_runner.daemon_context.working_directory=router.config['RUN_DIR']
    daemon_runner.do_action()
