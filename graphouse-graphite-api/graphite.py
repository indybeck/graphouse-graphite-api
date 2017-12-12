import json
import time
import traceback
import requests

from six.moves.urllib import parse
from six.moves import xrange
from structlog import get_logger

from graphite_api.intervals import IntervalSet, Interval
from graphite_api.node import LeafNode, BranchNode

log = get_logger()

def graphouse_request(paths, start_time, end_time, graphouse_url):
    try:
        query = parse.urlencode(
                    {
                        'metrics': ','.join([path.replace('\'', '\\\'') for path in paths]),
                        'start': start_time,
                        'end': end_time,
                        'reqKey': 'empty'
                    })
        request_url = graphouse_url + "/metricData"
        response = requests.post(request_url, params=query)

        log.debug('graphouse_data_query: %s parameters %s' % (request_url, query))

        response.raise_for_status()
        return json.loads(response.text)
    except Exception as e:
        log.error("Failed to fetch data, got exception:\n %s" % traceback.format_exc())
        raise e

def load_data(paths, start_time, end_time, graphouse_url, reqkey='empty'):

    # Loading too many metrics at once can overflow http request headers so batch them in batches
    metrics_data = {}
    batch_size = 75
    for batch in (paths[pos:pos + batch_size] for pos in xrange(0, len(paths), batch_size)):
        metrics_data.update(graphouse_request(batch, start_time, end_time, graphouse_url))

    time_info = None
    series = {}

    for path in paths:
        metric_object = metrics_data.get(path)

        if metric_object is None:
            series[path] = []
        else:
            # WARNING: graphite_api only allows a single time_info for all results, we're using the first here but
            # that may not actually be correct.
            if time_info is None:
                time_info = (metric_object.get("start"), metric_object.get("end"), metric_object.get("step"))

            series[path] = metric_object.get("points", [])

    if time_info is None:
        time_info = (0, 0, 1)

    return time_info, series

class GraphouseLeafNode(LeafNode):
    __fetch_multi__ = 'graphouse'


class GraphouseFinder(object):
    __fetch_multi__ = 'graphouse'

    def __init__(self, config=None):
        config.setdefault('graphouse', {})
        self.graphouse_url = config['graphouse'].get('url', 'http://localhost:2005')

    def find_nodes(self, query):
        request = requests.get('%s/search?%s' % (self.graphouse_url, parse.urlencode({'query': query.pattern})))
        request.raise_for_status()
        result = request.text.split('\n')

        for metric in result:
            if not metric:
                continue
            if metric.endswith('.'):
                yield BranchNode(metric[:-1])
            else:
                yield GraphouseLeafNode(metric, GraphouseReader(metric, self.graphouse_url))

    def fetch_multi(self, nodes, start_time, end_time):
        paths = [node.path for node in nodes]
        return load_data(paths, start_time, end_time, self.graphouse_url)

class GraphouseReader(object):
    __slots__ = ('path', 'graphouse_url')

    def __init__(self, path, graphouse_url):
        self.path = path
        self.graphouse_url = graphouse_url

    def get_intervals(self):
        return IntervalSet([Interval(0, int(time.time()))])

    def fetch(self, start_time, end_time):
        time_info, series = load_data([self.path], start_time, end_time, self.graphouse_url) 
        return time_info, series.get(self.path, [])
