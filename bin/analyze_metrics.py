#!/usr/bin/env python
from copy import deepcopy
# Analayze metrics in input_file of the form:
#2017-11-17 00:33:46,225 DEBUG METRICS resourceid=xstream.stanford.xsede.org 15/items: jobs/item=2925, to_receive=13.5759758358, to_beginprocess=46.4138246667, to_process=14.295467
#2017-11-17 00:33:46,225 INFO METRICS AVERAGE 115/items: to_receive=7.14893241388, to_beginprocess=37.0514524307, to_process=15.7742226155, end-to-end=59.9746074601

def print_sublist (type, sublist):
    print('{0}:'.format( type ))
    try:
        l = sublist['to_receive=']
        print('  Queue receive latency    {0:8.2f}'.format( round(sum(l) / len(l), 2) ))
    except:
        pass
    try:
        l = sublist['to_beginprocess=']
        print('  Queue process latency    {0:8.2f}'.format( round(sum(l) / len(l), 2) ))
    except:
        pass
    try:
        l = sublist['to_process=']
        print('  Queue to jobs latency    {0:8.2f}'.format( round(sum(l) / len(l), 2) ))
    except:
        pass
    try:
        l = sublist['end-to-end=']
        print('  End to End latency       {0:8.2f}'.format( round(sum(l) / len(l), 2) ))
    except:
        pass

input_file = '/soft/warehouse-apps-1.0/Manage-Jobs/var/route_jobs.log'
stats = {}
of_interest = {'to_receive=': [], 'to_beginprocess=': [], 'to_process=': [], 'end-to-end=': []}
import pdb

with open(input_file, 'r') as input:
    for line in input:
        fields = line.split()
        if fields[3] != 'METRICS':                    # Only METRICS lines
            continue
#       print(len(fields), repr(fields))
        which = fields[4]
#       pdb.set_trace()
        if which not in stats:
	    stats[which] = deepcopy(of_interest)
        for i in range(5, len(fields)):           # Remaining fields
            for prefix in of_interest:
                if fields[i].startswith(prefix):
                    try:
                        val = float(fields[i][len(prefix):].strip(' ,'))
                        stats[which][prefix].append(val)
                    except:
                        pass
input.close()

average_sublist = None
for key in stats:
    if key == 'AVERAGE':
        average_sublist = stats[key]
        continue
    print_sublist( key, stats[key] )
print_sublist( 'AVERAGE', average_sublist )
