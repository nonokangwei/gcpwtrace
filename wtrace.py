import argparse
import datetime
import time
import pandas as pd
import configparser
import os
from math import ceil
import subprocess
from concurrent.futures import ThreadPoolExecutor
from google.cloud import bigquery


def get_wtrace_agent_list(bq_client, country_code, table_id, ip_version='ipv4', number_of_agents=0):
    # Init Query
    query = """
    SELECT distinct any_value(ip_address) over (partition by metro,isp) as address, country_code, isp, metro
    FROM `{}`
    WHERE country_code in ({}) and ip_version="{}"
""".format(table_id, country_code, ip_version)
    print(query)
    query_job = bq_client.query(query)
    df = query_job.to_dataframe()
    if number_of_agents > 0:
        df_full = pd.DataFrame()
        for country in df.country_code.unique():
            df_full = df_full.append(
                df[df.country_code == country].sample(n=number_of_agents, replace=True).drop_duplicates(), ignore_index=True)
        df = df_full
    rows = df
    return rows

def extract_result(lines):
    if len(lines) < 7:
        raise Exception('error result format, expected length: 7, get length: ', len(lines))
    
    result = {}
    result['agent_ip'] = lines[0].split(':')[1].split()[0].strip()
    result['agent_asn'] = lines[0].split(':')[2].split()[0].strip()
    result['dns_latency_us'] = int(lines[1].split(':')[1].split()[0].strip())
    result['remote_ip'] = lines[2].split('Using IP address:')[1].split()[0].strip()
    result['remote_ip_info'] = lines[2].split('Using IP address:')[1].split()[1].strip().replace('(','').replace(')','')
    result['tcp_latency_us'] = int(lines[3].split(':')[1].split()[0].strip())
    result['gcp_pop'] = None
    result['age'] = None
    #different agent return following line in different order, need to check keyword to map to current field
    for line_no in range(4,len(lines)):
        if lines[line_no].lower().find('rtt') != -1:
            result['ping_min_latency_ms'] = float(lines[line_no].split('=')[1].replace('ms', '').split('/')[0].strip())
            result['ping_avg_latency_ms'] = float(lines[line_no].split('=')[1].replace('ms', '').split('/')[1].strip())
            result['ping_max_latency_ms'] = float(lines[line_no].split('=')[1].replace('ms', '').split('/')[2].strip())
        elif lines[line_no].lower().find('cdn_cache_id') != -1:
            result['gcp_pop'] = lines[line_no].split(':')[1].split()[0].strip()
        elif lines[line_no].lower().find('age') != -1:
            result['age'] = lines[line_no].split(':')[1].split()[0].strip()
        elif lines[line_no].lower().find('response code') != -1:
            result['response_code'] = int(lines[line_no].split(':')[1].strip())
        elif lines[line_no].lower().find('response length') != -1:
            result['response_length'] = int(lines[line_no].split(':')[1].split(' ')[1].strip())
        elif lines[line_no].lower().find('after tcp handshake') != -1:
            result['http_latency_us'] = int(lines[line_no].split(':')[1].split()[0].strip())

    return result
 
def run_wtrace(dest_url, bq_client, table_id, agent_ip, run_info, pid, agent_num, loop):
    wtrace_cmd = '/google/data/ro/teams/internetto/wtrace --nowait --ip_version=4 --http_max_transfer_sec=360 --agent={} {}'.format(agent_ip['address'], dest_url)
    # update below to collect more useful info
    wtrace_cmd += ' | grep -i "Response code\|Time elapsed\|The trace ran on\|rtt min/avg/max\|Using IP address\|Cdn_Cache_Id\|Response length\|age"'
    try:
        wtrace_output = subprocess.run(wtrace_cmd, stdout=subprocess.PIPE, shell=True, universal_newlines=True)
        ret_splitlines = wtrace_output.stdout.splitlines()
        latency_result = extract_result(ret_splitlines)
        run_info['isp'] = agent_ip['isp']
        run_info['metro'] = agent_ip['metro']
        run_info['country_code'] = agent_ip['country_code']
        latency_result.update(run_info)
        # To know whether it is the first loop, for CDN cache behavior
        latency_result.update({'loop': loop+1})
        print('[loop {}: {}/{}]: {}'.format(loop+1, pid, agent_num, latency_result))
        bq_client.insert_rows_json(table_id,[latency_result])
    except Exception as e:
        print('wtrace command output error: ', ret_splitlines, e)

def start_wtrace_thread_pool(max_worker, dest_url, bq_client, table_id, num_loop, agent_list, run_info, interval, max_hours):
    #set maximum run time to 1 day, due to gcert certificate is only valid in 19 hours
    agent_num = agent_list.shape[0]
    endtime = datetime.timedelta(hours=max_hours) + datetime.datetime.utcnow()
    for loop in range(num_loop):
        print('===================================== LOOP {} ====================================='.format(loop+1))
        pool = ThreadPoolExecutor(max_workers=max_worker, thread_name_prefix="wtrace_")
        pid = 1
        run_info['datetime'] = str(datetime.datetime.utcnow())
        for index, agent_ip in agent_list.iterrows():
            if datetime.datetime.utcnow() < endtime:
               # run_info['isp'] = agent_ip['isp']
               # run_info['metro'] = agent_ip['metro']
                pool.submit(run_wtrace, dest_url, bq_client, table_id, agent_ip, run_info, pid, agent_num, loop)
                pid += 1
        
        pool.shutdown(wait=True)
        time.sleep(interval)

def main():
    # Construct the argument parser
    ap = argparse.ArgumentParser()

    # Add the arguments to the parser
    ap.add_argument("-c", "--countrycode", required=True,
       help="2 letters country code, one or a list seperated by comma, example: US or US,JP,SG")
    ap.add_argument("-i", "--interval", required=True,
       help="set the trace interval between different loop")
    ap.add_argument("-p", "--parrallel", default=32,
       help="set concurrency which can not greater than 32, default 32")
    ap.add_argument("-e", "--endpoint", required=True,
       help="set trace destination with pattern (ping|http)://")
    ap.add_argument("-l", "--loop", default=1,
       help="set test loop number, default 1")
    ap.add_argument("-m", "--max_run_hours", default=19,
       help="set max run hours, default 19")
    ap.add_argument("-n", "--number_of_agents", default=0,
       help="set max agents per country, default 0 = no limit")
    args = vars(ap.parse_args())

    # Init variable
    country_code = ','.join(["\'" + x + "\'" for x in args["countrycode"].strip().replace(' ', '').split(',')])
    country_code = country_code.strip("\"")
    max_worker = int(args["parrallel"])
    interval = int(args["interval"])
    dest_url = args["endpoint"]
    num_loop = int(args["loop"])
    max_hours = int(args["max_run_hours"])
    number_of_agents = int(args["number_of_agents"])
    run_id = datetime.datetime.now().strftime('%Y%m%d%H%M%S')

    # Init tableid
    # latency_table_id = "dave-selfstudy01.wtrace.tx_result"         #<==== modify to your table
    # agent_table_id = "dave-selfstudy01.wtrace.agent_info"    #<==== modify to your table
    config = configparser.ConfigParser()
    config.read('config.ini')
    latency_table_id = config['TABLES']['latency_table_id']
    agent_table_id = config['TABLES']['agent_table_id']
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = config['SECRETS']['service_key_path']

    schema = [
        bigquery.SchemaField('run_id', 'STRING'),
        bigquery.SchemaField('agent_ip', 'STRING'),
        bigquery.SchemaField('agent_asn','STRING'),
        bigquery.SchemaField('isp','STRING'),
        bigquery.SchemaField('metro','STRING'),
        bigquery.SchemaField('remote_ip','STRING'),
        bigquery.SchemaField('remote_ip_info','STRING'),
        bigquery.SchemaField('dns_latency_us', 'INTEGER'),
        bigquery.SchemaField('ping_min_latency_ms','FLOAT'),
        bigquery.SchemaField('ping_avg_latency_ms','FLOAT'),
        bigquery.SchemaField('ping_max_latency_ms','FLOAT'),
        bigquery.SchemaField('tcp_latency_us', 'INTEGER'),
        bigquery.SchemaField('http_latency_us','INTEGER'),
        bigquery.SchemaField('response_code','INTEGER'),
        bigquery.SchemaField('response_length','INTEGER'),
        bigquery.SchemaField('country_code','STRING'),
        bigquery.SchemaField('datetime','DATETIME'),
        bigquery.SchemaField('gcp_pop','STRING'),
        bigquery.SchemaField('age','INTEGER'),
        bigquery.SchemaField('loop','INTEGER'),
        bigquery.SchemaField('dest_url','STRING')
    ]

    # Construct a BigQuery client object.
    client = bigquery.Client(location='US')
    #create table
    table = bigquery.Table(latency_table_id, schema=schema)
    client.create_table(table, exists_ok=True)

    agent_list = get_wtrace_agent_list(client, country_code, agent_table_id, 'ipv4', number_of_agents)

    run_info = {
        'run_id': run_id,
       # 'country_code': country_code,
       # 'datetime': str(datetime.datetime.utcnow()),
        'dest_url': dest_url
    }

    start_wtrace_thread_pool(max_worker, dest_url, client, latency_table_id, num_loop, agent_list, run_info, interval, max_hours)
    client.close()

if __name__ == '__main__':
    main()
