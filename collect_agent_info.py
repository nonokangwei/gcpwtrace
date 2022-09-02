import subprocess
import time
import configparser
import os
from google.cloud import bigquery

BATCH_SIZE = 1000

ret = subprocess.run(["/google/data/ro/teams/internetto/wtrace list_agents"] , stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, shell=True)
# print(type(ret.stdout))

retsplitlines = ret.stdout.splitlines()

# Init tableid
# table_id = "dave-selfstudy01.wtrace.agent_info"  #<==== modify to your table
config = configparser.ConfigParser()
config.read('config.ini')
table_id = config['TABLES']['agent_table_id']
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = config['SECRETS']['service_key_path']

schema = [
    bigquery.SchemaField('location', 'STRING'),
    bigquery.SchemaField('isp', 'STRING'),
    bigquery.SchemaField('metro', 'STRING'),
    bigquery.SchemaField('country_code','STRING'),
    bigquery.SchemaField('ip_address','STRING'),
    bigquery.SchemaField('ip_version','STRING')
]

# Construct a BigQuery client object.
client = bigquery.Client(location='us-central1')
#create table
table = bigquery.Table(table_id, schema=schema)
client.create_table(table, exists_ok=True)
# table schema:
# location:string,country:string,address:string,ip_version:string

# sampledata = """+-----------------------------+----------------------+---------+----------------------------------+
# | Location                    | Flooefi UUID         | Country | Egress IP                        |
# +-----------------------------+----------------------+---------+----------------------------------+
# | dnetservbd-dac1             | 3594737564943877970  | BD      | 160.20.72.4                      |
# | skbroadband-gmp35           | 1387816225470492911  | KR      | 1.255.2.200                      |
# | oteglobe-skg2               | 4453825814554698045  | GR      | 62.75.23.201                     |
# | nextel-gig2                 | 1634007820697822119  | BR      | 2804:138b:c040:a5::5             |
# | personalpy-asu3             | 12384480148939100018 | PY      | 2803:2a01:9800:1::7              |
# | tpnet-waw18                 | 6089390752246294822  | PL      | 46.134.193.75                    |
# | rjil-ixc4                   | 3706866381673947722  | IN      | 2405:200:160e:1726::4            |"""

# retsplitlines = sampledata.splitlines()



batch_idx = 1
rows_to_insert = []

for i in range(3,len(retsplitlines)-2):
    agent_result = {}
    
    agent_infor = retsplitlines[i]
    agent_inforsplit = agent_infor.split("|")
    agent_result["location"] = agent_inforsplit[1].strip()
    if agent_result["location"] != '':
        location_split = agent_result["location"].split('-')
        agent_result["isp"] = location_split[0].strip()
        agent_result["metro"] = location_split[1].strip()
        agent_result["country_code"] = agent_inforsplit[3].strip()
        agent_result["ip_address"] = agent_inforsplit[4].strip()
        if agent_inforsplit[4].find('.') >= 0:
            agent_result["ip_version"] = 'ipv4'
        else:
            agent_result["ip_version"] = 'ipv6'
        rows_to_insert.append(agent_result)
        # print(agent_result)
        if batch_idx % BATCH_SIZE == 0:
            errors = client.insert_rows_json(table_id,rows_to_insert)
            rows_to_insert = []
            time.sleep(3)
            if errors == []:
                print(batch_idx, "New rows have been added.")
            else:
                print("Encountered errors while inserting rows: {}".format(errors))

        batch_idx += 1

#insert remain records
errors = client.insert_rows_json(table_id,rows_to_insert)
if errors == []:
    print(batch_idx-1, "New rows have been added.")
else:
    print("Encountered errors while inserting rows: {}".format(errors))


client.close()
