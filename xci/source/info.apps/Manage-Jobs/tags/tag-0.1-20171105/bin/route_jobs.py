#!/usr/bin/env python

# Route computing_activity messages
#   and synchronize ComputingQueue Model state
#   into the ComputingActvities Model
# from a source (amqp, file, directory)
#   to a destination (print, directory, warehouse, api)

from __future__ import print_function
import amqp
import argparse
import base64
import datetime
from datetime import datetime, timedelta
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
from django.utils import timezone
from django.utils.dateparse import parse_datetime
from glue2_provider.process import Glue2ProcessRawIPF, StatsSummary
from glue2_db.models import ComputingActivity, ComputingQueue
from xsede_warehouse.stats import StatsTracker

from daemon import runner
import pdb

# Select Activity field cache
# New activities that match the cache aren't updated in the db to optimize performance
# The cache has a timestamp we can use to expire and reset the contents
a_cache = {}
a_cache_ts = timezone.now()

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
        parser.add_argument('-q', '--queue', action='store', default='jobs-router', \
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
        config_file = os.path.abspath(self.args.config)
        try:
            with open(config_file, 'r') as file:
                conf=file.read()
                file.close()
        except IOError as e:
            raise
        try:
            self.config = json.loads(conf)
        except ValueError as e:
            self.logger.error('Error "%s" parsing config=%s' % (e, config_file))
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
            raise ValueError('Invalid log level: %s' % numeric_log)
        self.logger = logging.getLogger('xsede.glue2')
        self.logger.setLevel(numeric_log)

        # Verify arguments and parse compound arguments
        if 'src' not in self.args or not self.args.src: # Tests for None and empty ''
            if 'SOURCE' in self.config:
                self.args.src = self.config['SOURCE']
        if 'src' not in self.args or not self.args.src:
            self.args.src = 'amqp:info1.dyn.xsede.org:5671'
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
            self.src['display'] = '%s@%s:%s' % (self.src['type'], self.src['host'], self.src['port'])
        elif self.src['obj']:
            self.src['display'] = '%s:%s' % (self.src['type'], self.src['obj'])
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
            self.dest['display'] = '%s@%s:%s' % (self.dest['type'], self.dest['host'], self.dest['port'])
        elif self.dest['obj']:
            self.dest['display'] = '%s:%s' % (self.dest['type'], self.dest['obj'])
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
                self.logger.error('Destination directory=%s not writable' % self.dest['obj'])
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
                self.pidfile_path =  '/var/run/%s/%s.pid' % (name ,name)

    def SaveDaemonLog(self, path):
        # Save daemon log file using timestamp only if it has anything unexpected in it
        try:
            with open(path, 'r') as file:
                lines=file.read()
                file.close()
                if not re.match("^started with pid \d+$", lines) and not re.match("^$", lines):
                    ts = datetime.strftime(datetime.now(), '%Y-%m-%d_%H:%M:%S')
                    newpath = '%s.%s' % (path, ts)
                    shutil.copy(path, newpath)
                    print('SaveDaemonLog as ' + newpath)
        except Exception as e:
            print('Exception in SaveDaemonLog({})'.format(path))
        return

    def exit_signal(self, signal, frame):
        self.logger.error('Caught signal, exiting...')
        sys.exit(0)

    def ConnectAmqp_Anonymous(self):
        return amqp.Connection(host='%s:%s' % (self.src['host'], self.src['port']), virtual_host='xsede')
    #                           heartbeat=2)

    def ConnectAmqp_UserPass(self):
        ssl_opts = {'ca_certs': os.environ.get('X509_USER_CERT')}
        return amqp.Connection(host='%s:%s' % (self.src['host'], self.src['port']), virtual_host='xsede',
                               userid=self.config['AMQP_USERID'], password=self.config['AMQP_PASSWORD'],
    #                           heartbeat=1,
                               heartbeat=240,
                               ssl=ssl_opts)

    def ConnectAmqp_X509(self):
        ssl_opts = {'ca_certs': self.config['X509_CACERTS'],
                   'keyfile': '/path/to/key.pem',
                   'certfile': '/path/to/cert.pem'}
        return amqp.Connection(host='%s:%s' % (self.src['host'], self.src['port']), virtual_host='xsede',
    #                           heartbeat=2,
                               ssl=ssl_opts)

    def src_amqp(self):
        return

    def amqp_callback(self, message):
        st = datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ')
        doctype = message.delivery_info['exchange']
        tag = message.delivery_tag
        resourceid = message.delivery_info['routing_key']
        if self.dest['type'] == 'print':
            self.dest_print(st, doctype, resourceid, message.body)
        elif self.dest['type'] == 'directory':
            self.dest_directory(st, doctype, resourceid, message.body)
        elif self.dest['type'] == 'warehouse':
            self.dest_warehouse(st, doctype, resourceid, message.body)
        elif self.dest['type'] == 'api':
            self.dest_restapi(st, doctype, resourceid, message.body)
        self.channel.basic_ack(delivery_tag=tag)

    def dest_print(self, st, doctype, resourceid, message_body):
        print('{} exchange={}, routing_key={}, size={}, dest=PRINT'.format(st, doctype, resourceid, len(message_body) ) )
        if self.dest['obj'] != 'dump':
            return
        try:
            py_data = json.loads(message_body)
        except ValueError as e:
            self.logger.error('Parsing Exception: %s' % (e))
            return
        for key in py_data:
            print('  Key=' + key)

    def dest_directory(self, st, doctype, resourceid, message_body):
        dir = os.path.join(self.dest['obj'], doctype)
        if not os.access(dir, os.W_OK):
            self.logger.critical('%s exchange=%s, routing_key=%s, size=%s Directory not writable "%s"' %
                  (st, doctype, resourceid, len(message_body), dir ) )
            return
        file_name = resourceid + '.' + st
        file = os.path.join(dir, file_name)
        self.logger.info('%s exchange=%s, routing_key=%s, size=%s dest=file:<exchange>/%s' %
                  (st, doctype, resourceid, len(message_body), file_name ) )
        with open(file, 'w') as fd:
            fd.write(message_body)
            fd.close()

    def dest_restapi(self, st, doctype, resourceid, message_body):
        if doctype in ['glue2.computing_activity']:
            self.logger.info('exchange=%s, routing_key=%s, size=%s dest=DROP' %
                  (doctype, resourceid, len(message_body) ) )
            return

        headers = {'Content-type': 'application/json',
            'Authorization': 'Basic %s' % base64.standard_b64encode( self.config['API_USERID'] + ':' + self.config['API_PASSWORD']) }
        url = '/glue2-provider-api/v1/process/doctype/%s/resourceid/%s/' % (doctype, resourceid)
        if self.dest['host'] not in ['localhost', '127.0.0.1'] and self.dest['port'] != '8000':
            url = '/wh1' + url
        (host, port) = (self.dest['host'].encode('utf-8'), self.dest['port'].encode('utf-8'))
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
                self.logger.debug('POST %s' % url)
                conn.request('POST', url, message_body, headers)
                response = conn.getresponse()
                self.logger.info('RESP exchange=%s, routing_key=%s, size=%s dest=POST http_response=status(%s)/reason(%s)' %
                    (doctype, resourceid, len(message_body), response.status, response.reason ) )
                data = response.read()
                conn.close()
                break
            except (socket.error) as e:
                retries += 1
                sleepminutes = 2*retries
                self.logger.error('Exception socket.error to %s:%s; sleeping %s/minutes before retrying' % \
                                  (host, port, sleepminutes))
                sleep(sleepminutes*60)
            except (httplib.BadStatusLine) as e:
                retries += 1
                sleepminutes = 2*retries
                self.logger.error('Exception httplib.BadStatusLine to %s:%s; sleeping %s/minutes before retrying' % \
                                  (host, port, sleepminutes))
                sleep(sleepminutes*60)

        if response.status in [400, 403]:
            self.logger.error('response=%s' % data)
            return
        try:
            obj = json.loads(data)
        except ValueError as e:
            self.logger.error('API response not in expected format (%s)' % e)

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
            self.logger.error('Parsing "%s" Exception: %s' % (path, e))
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
        import pdb
        # Frequency to load ComputingQueue (our input)
        Queue_RefreshInterval = timedelta(seconds=10)
        Queue_NextRefresh = datetime.now() - Queue_RefreshInterval # Force initial refresh
        Queue = {}
        Processed = {}

        while True:
            LoopStart = datetime.now()
            Processed_Count = 0
            
            if LoopStart > Queue_NextRefresh:
                objects = ComputingQueue.objects.all()
                for obj in objects:
                    sortkey = '{:%Y-%m-%d %H:%M:%S.%f}:{}'.format(obj.CreationTime, obj.ResourceID)
                    Queue[sortkey] = obj
                Queue_NextRefresh = datetime.now() + Queue_RefreshInterval
        
            for key in sorted(Queue):
                ResourceID = Queue[key].ResourceID
                CreationTime = Queue[key].CreationTime
                if ResourceID in Processed and Processed[ResourceID] == CreationTime:
                    continue # No change since last processed
                Jobs = Queue[key].EntityJSON
                self.process_jobs(ResourceID, CreationTime, Jobs)
                Processed[ResourceID] = CreationTime
                Processed_Count += 1
                if datetime.now() > Queue_NextRefresh: # Let's loop around and refresh the Queue
                    break
            else: # We've finished the sorted(Queue)
                LoopEnd = datetime.now()
                SleepFor = max(0.01, (Queue_NextRefresh - datetime.now()).total_seconds()) # No less than a 1/100 sec.
                self.logger.info('Loop Start={:%H:%M:%S.%f}, Refresh={:%H:%M:%S.%f}, Processed={}, Sleeping={}'.format(LoopStart, Queue_NextRefresh, Processed_Count, SleepFor))
                try:
                    sleep(SleepFor)
                except KeyboardInterrupt:
                    raise
                except Exception:
                    self.logger.error('Failed to sleep for "{}"'.format(SleepFor))
                continue # To outer loop
            # We should only fall thru if we break above (passed the next refresh)
            LoopEnd = datetime.now()
            self.logger.info('Loop Start={:%H:%M:%S.%f}, Refresh={:%H:%M:%S.%f}, Processed={}'.format(LoopStart, Queue_NextRefresh, Processed_Count))

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

        self.new[me] = Jobs
        self.stats.set('%s.New' % me, len(Jobs))

        # Load current database entries
        if not self.cur[me]:
            for item in ComputingActivity.objects.filter(ResourceID=self.resourceid):
                self.cur[me][item.ID] = item
            self.stats.set('%s.Current' % me, len(self.cur[me]))

        # Add/update entries
        for ID in self.new[me]:
            if ID in self.cur[me] and parse_datetime(self.new[me][ID]['CreationTime']) <= self.cur[me][ID].CreationTime:
                self.new[me][ID]['model'] = self.cur[me][ID]    # Save the latest object reference
                continue                                        # Don't update database since is has the latest

            if self.activity_is_cached(ID, self.new[me][ID]):
                self.stats.add('%s.ToCache' % me, 1)
                continue

            try:
                model = ComputingActivity(ID=self.new[me][ID]['ID'],
                                          ResourceID=self.resourceid,
                                          Name=self.new[me][ID].get('Name', 'none'),
                                          CreationTime=self.new[me][ID]['CreationTime'],
                                          Validity=get_Validity(self.new[me][ID]),
                                          EntityJSON=self.new[me][ID])
                model.save()
                self.new[me][ID]['model'] = model
                self.stats.add('%s.Updates' % me, 1)

                self.activity_to_cache(ID, self.new[me][ID])
            
            except (DataError, IntegrityError) as e:
                raise ProcessingException('%s updating %s (ID=%s): %s' % (type(e).__name__, me, self.new[me][ID]['ID'], \
                                        e.message), status=status.HTTP_400_BAD_REQUEST)

        # Delete old entries
        for ID in self.cur[me]:
            if ID in self.new[me]:
                continue
            try:
                ComputingActivity.objects.filter(ID=ID).delete()
                self.cur[me].pop(ID)
                self.stats.add('%s.Deletes' % me, 1)
            except (DataError, IntegrityError) as e:
                raise ProcessingException('%s deleting %s (ID=%s): %s' % (type(e).__name__, me, ID, e.message), \
                                          status=status.HTTP_400_BAD_REQUEST)

        self.stats.end()
        self.logger.info(self.stats.summary())

    def activity_is_cached(self, id, obj): # id=object unique id
        global a_cache
        global a_cache_ts
        if (timezone.now() - a_cache_ts).total_seconds() > 3600:    # Expire cache every hour (3600 seconds)
            a_cache_ts = timezone.now()
            a_cache = {}
            logg2.debug('Expiring Activity cache')

        return id in a_cache and a_cache[id] == self.activity_hash(obj)

    def activity_to_cache(self, id, obj):
        global a_cache
        a_cache[id] = self.activity_hash(obj)

    def activity_hash(self, obj):
        hash_list = []
        if 'State' in obj:
            hash_list.append(obj['State'])
        if 'UsedTotalWallTime' in obj:
            hash_list.append(obj['UsedTotalWallTime'])
        return(json.dumps(hash_list))

###############################################################################################
# Where we process
###############################################################################################
    def run(self):
        signal.signal(signal.SIGINT, self.exit_signal)

        self.logger.info('Starting program={} pid={}, uid={}({})'.format(os.path.basename(__file__), os.getpid(), os.geteuid(), pwd.getpwuid(os.geteuid()).pw_name))
        self.logger.info('Source: ' + self.src['display'])
        self.logger.info('Destination: ' + self.dest['display'])

        if self.src['type'] == 'amqp':
            conn = self.ConnectAmqp_UserPass()
            self.channel = conn.channel()
            self.channel.basic_qos(prefetch_size=0, prefetch_count=4, a_global=True)
            declare_ok = self.channel.queue_declare(queue=self.args.queue, durable=True, auto_delete=False)
            queue = declare_ok.queue
            exchanges = ['glue2.computing_activities']
            for ex in exchanges:
                self.channel.queue_bind(queue, ex, '#')
            self.logger.info('AMQP Queue={}, Exchanges=({})'.format(self.args.queue, ', '.join(exchanges)))
            st = datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ')
            self.channel.basic_consume(queue,callback=self.amqp_callback)
            while True:
                self.channel.wait()

        elif self.src['type'] == 'file':
            self.src['obj'] = os.path.abspath(self.src['obj'])
            if not os.path.isfile(self.src['obj']):
                self.logger.error('Source is not a readable file=%s' % self.src['obj'])
                sys.exit(1)
            self.process_file(self.src['obj'])

        elif self.src['type'] == 'directory':
            self.src['obj'] = os.path.abspath(self.src['obj'])
            if not os.path.isdir(self.src['obj']):
                self.logger.error('Source is not a readable directory=%s' % self.src['obj'])
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