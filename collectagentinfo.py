import subprocess
from google.cloud import bigquery

ret = subprocess.run(["/google/data/ro/teams/internetto/wtrace list_agents"] , stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, shell=True)
# print(type(ret.stdout))

retsplitlines = ret.stdout.splitlines()

# Init tableid
table_id = "project-wekang-poc.wtrace.agentinfor"

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

# Construct a BigQuery client object.
client = bigquery.Client()

for i in range(3,len(retsplitlines)-2):
    agent_result = {}
    rows_to_insert = []
    agent_infor = retsplitlines[i]
    agent_inforsplit = agent_infor.split("|")
#    print(agent_inforsplit)
    agent_result["location"] = agent_inforsplit[1].strip()
    agent_result["country"] = agent_inforsplit[3].strip()
    agent_result["address"] = agent_inforsplit[4].strip()
    if agent_inforsplit[4].find('.') >= 0:
        agent_result["ip_version"] = 'ipv4'
    else:
        agent_result["ip_version"] = 'ipv6'
    print(agent_result)
    rows_to_insert.append(agent_result)

    errors = client.insert_rows_json(table_id,rows_to_insert) # Make an API request.
    if errors == []:
        print("New rows have been added.")
    else:
        print("Encountered errors while inserting rows: {}".format(errors))

client.close()
