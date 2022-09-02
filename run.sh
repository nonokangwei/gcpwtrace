#!/bin/bash
# target1="http://47.254.70.156/" # west
# target2="http://47.253.54.30/" # east
target1="http://8.219.85.89/gcp_gaming.pdf" # SG
target2="http://8.209.247.102/gcp_gaming.pdf" # JP
target3="http://47.254.134.72/gcp_gaming.pdf" # Frankfurt
# target4="" # JP
# target5="" # Frankfurt
# target6="" # Frankfurt
# target7="" # Netherland

# country_code="JP,KR,TW,HK,SG,TH,VN,PH,ID,MY,US,DE,FR,GB,BR,RU,CA,IT,SA,QA,IN,ZA"
# country_code="VN,LA,TH,KH,MY,MM,SG,ID,PH,TW,HK,JP,KR,IN,BD,LK"
# country_code="AR,PY,PA,BR,PE,PR,BO,DO,EC,CO,CR,MX,NI,GT,VE,UY,CL"

country_code1="ID,TH,VN,MY,PH,SG,MM,KH,LA,BN"
country_code2="HK,TW,KR,JP,MO"
country_code3="GE,GB,FR,IT,RU,ES,NL,CH,PL,SE"

# country_code="US"
python -u wtrace.py -c ${country_code1} -i 5 -n 100 -p 32 -e ${target1}
python -u wtrace.py -c ${country_code2} -i 5 -n 100 -p 32 -e ${target2}
python -u wtrace.py -c ${country_code3} -i 5 -n 100 -p 32 -e ${target3}
# python -u wtrace.py -c ${country_code} -i 5 -l 1 -p 32 -e ${target}
