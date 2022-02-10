#!/bin/bash
#target1="http://d12x2nvgm9y53f.cloudfront.net/public/TencentMeeting.dmg"
target="https://www1.totorochina.com/public/TencentMeeting.dmg"
country_code="JP,KR,TW,HK,SG,TH,VN,PH,ID,MA,US,DE,FR,BR,RU,CA,IT"
# country_code="PK"
# target="https://coscloud.99.com/43f2bdd810c750ec7b7fedee2d1219fa/dcloud_default_component.zip"
python wtrace.py -c ${country_code} -i 5 -n 100 -l 5 -e ${target}
#python wtrace.py -c ${country_code} -i 5 -n 100 -l 5 -e ${target2}
