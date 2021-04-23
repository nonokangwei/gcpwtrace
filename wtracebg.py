import argparse
import datetime
import time
import pandas
from math import ceil
from subprocess import PIPE, run
from concurrent.futures import ThreadPoolExecutor
from google.cloud import bigquery

# sampledata = """The trace ran on: 62.214.98.9 (versatel-fra3:8881:xp)
#   Time elapsed: 183 usecs
#   Time elapsed: 981 usecs
#   Response code: 200
#   Time elapsed (after TCP handshake): 45466 usecs"""

# Construct the argument parser
ap = argparse.ArgumentParser()

# Add the arguments to the parser
ap.add_argument("-c", "--countrycode", required=True,
   help="please check out the available country code with --list countrycode")
ap.add_argument("-i", "--interval", required=True,
   help="set the trace interval")
ap.add_argument("-p", "--parrallel", required=True,
   help="set concurrency which can not greater than 32")
ap.add_argument("-e1", "--endpoint1", required=True,
   help="set trace destination with pattern http://")
ap.add_argument("-e2", "--endpoint2", required=True,
   help="set trace destination with pattern http://")
args = vars(ap.parse_args())

# Init variable
country_code = args["countrycode"]
max_worker = int(args["parrallel"])
interval = int(args["interval"])
dest_url_a = args["endpoint1"]
dest_url_b = args["endpoint2"]

def get_wtrace_agent_list(country_code, table_id, ip_version="ipv4"):
    # Construct a BigQuery client object.
    client = bigquery.Client()

    # Init Query
    query = """
    SELECT ARRAY_AGG(ip_address LIMIT 1)[OFFSET(0)] as address, country_code as country, isp, metro
    FROM `project-wekang-poc.wtrace.agentinfor`
    WHERE country_code = {} and ip_version={}
    GROUP BY isp, metro, country
""".format('"'+country_code+'"', '"'+ip_version+'"')

    query_job = client.query(query)
    rows = query_job.to_dataframe()
    
    # Close the BigQuery client object.
    client.close()

    return rows

def run_wtrace(country_code, dest_url, bg_client, table_id, agent_ip):
    latency_result = {}
    rows_to_insert = []
    wtrace_cmd = "/google/data/ro/teams/internetto/wtrace --nowait --ip_version=4 --agent=" + agent_ip["add
ress"] + " " + dest_url
#    cmd = 'ssh ' + glinux_host + ' "' + wtrace_cmd + '"' + '|grep "Response code\|Time elapsed\|The trace 
ran on"'
    cmd = wtrace_cmd  + '|grep "Response code\|Time elapsed\|The trace ran on"'
    print(cmd)
    ret = run(cmd, stdout=PIPE, stderr=PIPE, universal_newlines=True, shell=True)
    # Parse the cmd result to linesç
    ret_splitlines = ret.stdout.splitlines()
    # print(ret_splitlines)
    # Parse the result with schema
    for i in range(0,len(ret_splitlines)):
        if i == 0:
            latency_result["agent_ip"] = ret_splitlines[i].split(":")[1].split()[0].strip()
        elif i == 1:
            latency_result["dns_latency"] = int(ret_splitlines[i].split(":")[1].split()[0].strip())
        elif i == 2:
            latency_result["tcp_latency"] = int(ret_splitlines[i].split(":")[1].split()[0].strip())
        elif i == 3:
            latency_result["response_code"] = int(ret_splitlines[i].split(":")[1].strip())
        elif i == 4:
            latency_result["http_latency"] = int(ret_splitlines[i].split(":")[1].split()[0].strip())
    # Append Countrycode and Datetime information
    latency_result["country_code"] = country_code
    latency_result["datetime"] = str(datetime.datetime.utcnow())
    latency_result["isp"] = agent_ip["isp"]
    latency_result["metro"] = agent_ip["metro"]
    latency_result["dest_url"] = dest_url
    print(latency_result)
    # Streaming the metric to BQ
    rows_to_insert.append(latency_result)
    errors = client.insert_rows_json(table_id,rows_to_insert) # Make an API request.
    if errors == []:
        print("New rows have been added.")
    else:
        print("Encountered errors while inserting rows: {}".format(errors))
    # Check out the lantency_result
    # Print(latency_result)



def start_wtrace_thread_pool(max_worker, country_code, dest_url, bg_client, table_id, num_loop, agent_list)
:
    if num_loop == 1:
        pool = ThreadPoolExecutor(max_workers=max_worker, thread_name_prefix="wtrace_")
        for index, agent_ip in agent_list.iterrows():
            pool.submit(run_wtrace, country_code, dest_url, bg_client, table_id, agent_ip)

        pool.shutdown(wait=True)
        print('threadPool shutdown')
    else:
        # for j in range(0, num_loop):
        pool = ThreadPoolExecutor(max_workers=max_worker, thread_name_prefix="wtrace_")
        k = 0
        for index, agent_ip in agent_list.iterrows():
            pool.submit(run_wtrace, country_code, dest_url, bg_client, table_id, agent_ip)
            k = k + 1
            if k % 30 == 0 or k == agent_list.shape[0]:
                pool.shutdown(wait=True)
                print('threadPool shutdown')
                pool = ThreadPoolExecutor(max_workers=max_worker, thread_name_prefix="wtrace_")

# Init tableid
latency_table_id = "project-wekang-poc.wtrace.latencymetric"
agent_table_id = "project-wekang-poc.wtrace.agentinfor"

agent_list = get_wtrace_agent_list(country_code, agent_table_id)
print(agent_list.shape[0])
# Init max_worker
if agent_list.shape[0] > 30:
    max_worker = 30
    num_loop = int(ceil(agent_list.shape[0]/30))
else:
    max_worker = agent_list.shape[0]
    num_loop = 1

print(num_loop)

endtime = datetime.timedelta(days=1) + datetime.datetime.utcnow()
while(datetime.datetime.utcnow() <= endtime):
    # Construct a BigQuery client object.
    client = bigquery.Client()
    # Start the metricmonitor test
    start_wtrace_thread_pool(max_worker, country_code, dest_url_a, client, latency_table_id, num_loop, agen
t_list)
    start_wtrace_thread_pool(max_worker, country_code, dest_url_b, client, latency_table_id, num_loop, agen
t_list)
    # Wait for interval
    time.sleep(60)
    # Close the BigQuery client object.
    client.close()
