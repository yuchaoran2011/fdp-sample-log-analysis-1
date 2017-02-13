%spark.pyspark

"""
Log analysis application that processes data in Apache server log format.
Example datasets can be found in:
http://ita.ee.lbl.gov/html/contrib/ClarkNet-HTTP.html
This code is intended to be executed interactively in a Zeppelin session.
Please specify input file (i.e. log_file variable) before running.
"""

import datetime
import os
import re
import sys

from pyspark.sql import Row

import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt


# Specify input file here
log_file = 'path_to_input_file'

APACHE_LOG_REGEX = '^(\S+) (\S+) (\S+) \[([\w:/]+\s[+\-]\d{4})\] "(\S+) (\S+)\s*(\S*)" (\d{3}) (\S+)'
months = {'Jan': 1, 'Feb': 2, 'Mar':3, 'Apr':4, 'May':5, 'Jun':6, 'Jul':7, 'Aug':8,  'Sep': 9, 'Oct':10, 'Nov': 11, 'Dec': 12}


def parse_apache_log_time(s):
    return datetime.datetime(int(s[7:11]), months[s[3:6]], int(s[0:2]), int(s[12:14]), int(s[15:17]), int(s[18:20]))


def parse_apache_log(line):
    match = re.search(APACHE_LOG_REGEX, line)
    if match is None:
        return (line, 0)
    payload_size_field = match.group(9)
    if payload_size_field == '-':
        payload_size = long(0)
    else:
        payload_size = long(match.group(9))
    return (Row(
        ip            = match.group(1),
        client_ic     = match.group(2),
        user          = match.group(3),
        date_time     = parse_apache_log_time(match.group(4)),
        method        = match.group(5),
        endpoint      = match.group(6),
        protocol      = match.group(7),
        status        = int(match.group(8)),
        payload_size  = payload_size
    ), 1)


def parse_apache_logs():
    parsed_logs = sc.textFile(log_file).map(parse_apache_log).cache()

    access_logs = parsed_logs.filter(lambda s: s[1] == 1).map(lambda s: s[0]).cache()
    failed_logs = parsed_logs.filter(lambda s: s[1] == 0).map(lambda s: s[0])

    failed_logs_count = failed_logs.count()
    if failed_logs_count > 0:
        print 'Number of invalid logs: %d' % failed_logs_count

    print 'Parsed %d lines. Successfully parsed %d lines. Failed to parse %d lines' % (parsed_logs.count(), access_logs.count(), failed_logs_count)
    return parsed_logs, access_logs, failed_logs


parsed_logs, access_logs, failed_logs = parse_apache_logs()

# Output statistics on request payload sizes
payload_sizes = access_logs.map(lambda log: log.payload_size).cache()
payload_size_avg = payload_sizes.reduce(lambda a, b : a + b) / payload_sizes.count()
print 'Payload Size Avg: %i, Min: %i, Max: %s' % (payload_size_avg, payload_sizes.min(), payload_sizes.max())


# Generate variables for use in plotting
status_counts = access_logs.map(lambda log: (log.status, 1)).reduceByKey(lambda a, b: a + b).cache()
labels = status_counts.map(lambda (x, y): x).collect()
count = access_logs.count()
fractions = status_counts.map(lambda (x, y): (float(y) / count)).collect()


# Plot a pie chart of status code counts
def pie_chart_format(value):
    return '' if value < 7 else '%.0f%%' % value

fig = plt.figure(figsize=(4.5, 4.5), facecolor='white', edgecolor='white')
colors = ['yellow', 'green', 'gold', 'purple', 'blue', 'yellow', 'black']
explode = (0.05, 0.05, 0.1, 0, 0, 0, 0)
patches, texts, autotexts = plt.pie(fractions, labels=labels, colors=colors,
                                    explode=explode, autopct=pie_chart_format,
                                    shadow=False,  startangle=125)
for text, autotext in zip(texts, autotexts):
    if autotext.get_text() == '':
        text.set_text('')
plt.legend(labels, loc=(0.80, -0.1), shadow=True)

# Output any 10 hosts that appeared more than 20 times
ip_counts = access_logs.map(lambda log: (log.ip, 1))
host_more_than_20 = ip_counts.reduceByKey(lambda a, b : a + b).filter(lambda s: s[1] > 20)
hosts_pick_10 = host_more_than_20.map(lambda s: s[0]).take(10)
print 'Some 10 hosts that have accessed more then 20 times: %s' % hosts_pick_10
