import json
import time
import requests
import logging
import os

from kazoo.client import KazooClient

ZOOKEEPER_HOSTS = '127.0.0.1:2181'
SERVICE_NAME = 'Easydb'
LOAD_BALANCERS_STR = '127.0.0.1:8001'

try:
    ZOOKEEPER_HOSTS = os.environ['ZOOKEEPER_HOSTS']
except:
    pass

try:
    SERVICE_NAME = os.environ['SERVICE_NAME']
except:
    pass

try:
    LOAD_BALANCERS_STR = os.environ['LOAD_BALANCERS']
except:
    pass

LOAD_BALANCERS = ";".join(LOAD_BALANCERS_STR)

SLEEP_TIME_SECONDS = 1

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
FORMAT = '%(asctime)s - %(levelname)s - %(message)s'
logging.basicConfig(level=logging.INFO, format=FORMAT)

zk = KazooClient(hosts=ZOOKEEPER_HOSTS, read_only=True)
zk.start()

def read_services():
    if not zk.exists(services_path(SERVICE_NAME)):
        return []

    children = zk.get_children(services_path(SERVICE_NAME))
    services = []
    for child in children:
        instance = parse_instance(zk.get(service_instance_path(SERVICE_NAME, child))[0])
        services.append(instance)
    return services

def services_path(service_name):
    return '/discovery/%s' % service_name

def service_instance_path(service_name, instance_name):
    return "%s/%s" % (services_path(service_name), instance_name)

def parse_instance(instance_data: bytes):
    data_as_dict = json.loads(instance_data)
    return "%s:%s" % (data_as_dict['address'], data_as_dict['port'])

def send_instances_to_load_balancer(services):
    data = ""
    success = True
    for s in services:
        data += "server %s;" % s

    for load_balancer in LOAD_BALANCERS:
        response = requests.post("http://%s/upstream/backend" % load_balancer, data)
        if response.status_code >= 300:
            logger.warning("Failed to pass services ips to load balancer %s. Status code %s", load_balancer, response.status_code)
            success = False

    return success

def run():
    previous_backends = []
    load_balancers_notified_correctly = False
    while True:
        try:
            time.sleep(SLEEP_TIME_SECONDS)
            current_backends = read_services()
            if current_backends != previous_backends or not load_balancers_notified_correctly:
                previous_backends = current_backends
                load_balancers_notified_correctly = send_instances_to_load_balancer(current_backends)
            logger.info("Found active backends: [%s]" % ", ".join(current_backends))
        except Exception as e:
            logger.exception("Failed to perform discovery scrapping %s", e)

run()