#!/bin/bash
target="https://www1.totorochina.com/public/TencentMeeting.dmg"
# country_code="JP,KR,TW,HK,SG,TH,VN,PH,ID,MA,US,DE,FR,BR,RU,CA,IT"
country_code="HK,TW,SG"
python wtrace.py -c ${country_code} -i 5 -n 100 -l 5 -e ${target}
