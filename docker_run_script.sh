#!/bin/bash
today_date=`date +%Y%m%d`
echo $today_date

echo "Run script launched"
/etc/init.d/cron start

app_location="/usr/app"
# installing requirements
# pip install -r requirements.txt
echo 'Starting Data Ingestion'
python3 ${app_location}/DataIngestion/main.py | tee

#echo 'Starting Smartabase Data Ingestion'
#python3 ${app_location}/DataIngestion/preprocessor/smartabase.py | tee
echo 'running app now'
python3 ${app_location}/boot_script/check_scheduler.py
python3 ${app_location}/DataService/src/app.py | tee
while true; do sleep 1000; done