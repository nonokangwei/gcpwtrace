import subprocess

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

ret = subprocess.run(["/google/data/ro/teams/internetto/wtrace list_agents"] , stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, shell=True)
# print(type(ret.stdout))

retsplitlines = ret.stdout.splitlines()
# for line in ret.stdout.splitlines():
#     print(line)

# retsplitlines = sampledata.splitlines()

f = open('nagentinfor.csv', 'w')

for i in range(3,len(retsplitlines)-2):
    agentinfor = retsplitlines[i]
    print(agentinfor)
    agentinforsplit = agentinfor.split("|")
    if agentinforsplit[1].strip() == "":
      metro = ""
      isp = ""
    else:
#      print(agentinforsplit[1])
      metro = agentinforsplit[1].split("-")[0].strip()
      isp = agentinforsplit[1].split("-")[1].strip()[0:3]
    agentinforresult = metro + ',' + isp + ',' + agentinforsplit[2].strip() + ',' + agentinforsplit[3].strip() + ',' + agentinforsplit[4].strip() + ','
    if agentinforsplit[4].find('.') >= 0:
        agentinforresult = agentinforresult + 'ipv4\n'
    else:
        agentinforresult = agentinforresult + 'ipv6\n'
    f.write(agentinforresult)
    # print(agentinforresult)
f.close()
