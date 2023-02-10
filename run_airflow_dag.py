import re 
import requests 
from requests.auth import HTTPBasicAuth
from datetime import datetime as dt

def run_session_gate_dag(airflow_url, username, password, dag_id , conf, run_id):
    auth = HTTPBasicAuth(username, password)
    # TO LOCAL TESTS
    data = {
        'conf': conf,
        "run_id": run_id,
    }
    connection = {
        "proxies" : {'http': 'socks5h://199.999.99.9:9999', 'https': 'socks5h://199.999.99.9:9998'},# ONLY FOR LOCAL TRIGGER
        "dag_url" : f"{airflow_url}/api/experimental/dags/{dag_id}/dag_runs",
        'data': data,
    }
    try:
        # res7 = requests.post(connection['dag_url'],auth=auth,proxies=connection['proxies'], json=connection['data'], verify=False) # RUN DAG , json={} NECESSARY TO RUN DAG
        res7 = requests.post(connection['dag_url'],auth=auth,json=connection['data'], verify=False) # RUN DAG , json={} NECESSARY TO RUN DAG
    except  requests.exceptions.HTTPError as err:
        print( f'Bad Status Code {res7.status_code}')
    
    print( res7.status_code )
    res7_json = res7.json()
    print(res7_json)
    return res7.json()


# airflow_url = "https://airflow-prod.apps.devops.advantagedp.org/"
airflow_url = "https://airflow-url/"
username = 'username'
password = '************'
dag_id = 'name_of_dag)'
conf = {
        'json':'format'
        }
time_now = dt.now().isoformat('Z')
run_id = f"{dag_id}_{time_now}_{conf['session_id']}_reprocessingg"      
run_session_gate_dag(airflow_url, username, password, dag_id , conf, run_id )


# curl -u username:password -X POST https://airflow-url/api/experimental/dags/dag_id/dag_runs -d {}
# curl -u username:password -X POST https://airflow-url/api/experimental/dags/dag_id/dag_runs -d {}
# -d {} # IS NECESSARY  # 
