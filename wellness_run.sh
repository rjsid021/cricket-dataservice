#!/bin/bash
today_date=`date +%Y%m%d`
echo $today_date

echo "wellness run script launched"

app_location="/usr/app"

cd ${app_location}

echo 'Current directory --> '${PWD}
export PYTHONPATH="${PWD}:${PYTHONPATH}"
# installing requirements
#pip3 install -r requirements.txt

echo 'fetching GPS data now'
curl http://localhost:5002/fetchLatestGPSData
#/usr/local/bin/python3 ${app_location}/DataService/src/app.py | tee
while true; do sleep 1000; done