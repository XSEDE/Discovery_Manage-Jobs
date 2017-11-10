#!/usr/bin/env python
# Analayze metrics in input_file of the form:
# 2017-11-10 05:18:02,544 INFO METRICS 108/items: to_receive=7.23729864999, to_beginprocess=25.5092452334, to_process=14.503572165, end-to-end=47.2501160484

input_file = '/soft/warehouse-apps-1.0/Manage-Jobs/var/django_xsede_warehouse.log'
of_interest = {'to_receive=': [], 'to_beginprocess=': [], 'to_process=': [], 'end-to-end=': []}

with open(input_file, 'r') as input:
    for line in input:
        fields = line.split()
        if fields[3] == 'METRICS':                    # Only METRICS lines
#           print(len(fields), repr(fields))
            for i in range(4, len(fields)):           # Remaining fields
                for prefix in of_interest:
                    if fields[i].startswith(prefix):
                        try:
                            val = float(fields[i][len(prefix):].strip(' ,'))
                            of_interest[prefix].append(val)
			except:
                            pass
input.close()

l = of_interest['to_receive=']
print('Queue subscribe latency  {0:8.4f}'.format( (sum(l) / len(l)) ))
l = of_interest['to_beginprocess=']
print('Queue process latency    {0:8.4f}'.format( (sum(l) / len(l)) ))
l = of_interest['to_process=']
print('Jobs processed latency   {0:8.4f}'.format( (sum(l) / len(l)) ))
l = of_interest['end-to-end=']
print('TOTAL latency            {0:8.4f}'.format( (sum(l) / len(l)) ))
print('Note the SP publish latency')
