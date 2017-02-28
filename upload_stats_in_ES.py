import json
import sys

import requests


def upload_stats_es(metrics, test_name):
    stats_to_add = {}

    for key in metrics['stats'][0].keys():
        summary = 0
        for stat in metrics['stats']:
            try:
                summary += float(stat[key])
            except:
                stats_to_add[key] = stat[key]
        if summary != summary:
            stats_to_add[key] = None
        elif key not in stats_to_add:
            stats_to_add[key] = round(summary / len(metrics['stats']), 1)

    result = {}
    for k, v in metrics.iteritems():
        if k == 'stats':
            continue
        value = v
        try:
            value = int(v)
        except:
            try:
                value = float(v)
            except:
                pass

        result[k] = value

    result.update(stats_to_add)

    with open('jenkins_%s_summary.json' % test_name, 'w') as fp:
        json.dump(result, fp, indent=4)

    url = 'https://6cde69d70bffe5f26ff40d83ec6fcb26.eu-west-1.aws.found.io:9243/performanceregressiontest/%s/%s_%s?pretty' % \
          (result['test_name'], result['ami_id_db_scylla_desc'], result['time_completed'])
    print url
    print json.dumps(result, indent=4)
    headers = {'Accept': 'application/json', 'Content-Type': 'application/json'}
    r = requests.post(url, data=open('jenkins_%s_summary.json' % test_name, 'rb'), headers=headers,
                      auth=('elastic', '9Q8IeyzqtMSpEXdOWYcr3eAU'))
    print r.content


def main():
    assert len(sys.argv[1:]) > 0, 'list of tests is required\n' \
                                 'for example:' \
                                 'python upload_stats_in_ES.py test_read test_write'

    for test_name in sys.argv[1:]:
        with open('jenkins_%s.json' % test_name, 'r') as fp:
            metrics = json.load(fp)
        upload_stats_es(metrics, test_name=test_name)


if __name__ == "__main__":
    main()

"""
custom date format is used in ES
curl -XPUT 'https://elastic:9Q8IeyzqtMSpEXdOWYcr3eAU@6cde69d70bffe5f26ff40d83ec6fcb26.eu-west-1.aws.found.io:9243/performanceregressiontest' -H 'Content-Type:application/json' -d'{
   "mappings":{
      "performanceregressiontest":{
         "properties":{
            "time_completed":{
               "type":"date",
               "format":"yyyy-MM-dd HH:mm"
            }
         }
      }
   }
}
"""
