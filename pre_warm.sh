#!/bin/bash
target="https://showbizapcdn.m2time.net/gameres/trialres/800829/1602/f0c611e2b983b55c53d4e66a6405590b/1644373265/lbres.zip"
country_code="JP,KR,TW,HK,SG,TH,VN,PH,PK,BD,KR,ID,IN,MA,US,DE,FR,BR,RU,CA,IT,ES,PL,RO,NL,BE,GR,CZ"
# country_code="PK"
# target="https://coscloud.99.com/43f2bdd810c750ec7b7fedee2d1219fa/dcloud_default_component.zip"
python wtrace.py -c ${country_code} -i 5 -n 10 -l 1 -e ${target}
#python wtrace.py -c ${country_code} -i 5 -n 100 -l 5 -e ${target2}
