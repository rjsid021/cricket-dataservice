#!/bin/bash
today_date=`date +%Y%m%d`
echo $today_date

echo "Smartabase Ingestion Run Script Launched"

app_location="/usr/app"

cd ${app_location}

echo 'Current directory --> '${PWD}
export PYTHONPATH="${PWD}:${PYTHONPATH}"
# installing requirements
#pip3 install -r requirements.txt

export http_proxy="http://jazwestproxy.jio.com:8080/"
export https_proxy="http://jazwestproxy.jio.com:8080/"
export no_proxy="10.160.137.0/24,*.cluster,istiod.istio-system.svc,jioaicr.jioindiawest.data.azurecr.io,10.144.122.54,localhost,10.168.130.0/24,10.140.102.0/24,10.168.134.133,nexus.rjil.ril.com:5101,istio-sidecar-injector.istio-system.svc,jioaicr.azurecr.io,.cluster,169.254.169.254,10.160.138.0/24,*.svc,.svc,istiod.istio-system,devopsartifact.jio.com,devops.jio.com,tibesbent02.bss.sit.jio.com,168.63.129.16,10.168.134.0/23,.local,cluster.local,devopsartifact.jio.com/ui/packages,wccsit.jio.com,konnectivity,*.local,10.4.0.0/16,127.0.0.1,10.168.131.0/24,10.168.134.132,jioai-spor-jioai-sports-rg-a9527b-e14e7ef0.hcp.jioindiawest.azmk8s.io,10.248.0.0/16"
echo 'Starting Data Ingestion'
/usr/local/bin/python3 ${app_location}/DataIngestion/preprocessor/smartabase.py | tee
