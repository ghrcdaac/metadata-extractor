import os
import sys
from dateutil.parser import parse

run_cumulus_task = None
if os.environ.get('CUMULUS_MESSAGE_ADAPTER_DIR'):
    sys.path.insert(0, os.environ.get('CUMULUS_MESSAGE_ADAPTER_DIR'))
    from run_cumulus_task import run_cumulus_task


def lambda_handler(event, context=None):
    print(event)
    config = event['config']
    provider = config['provider']
    collection = event['config']['collection']
    discover_tf = collection['meta']['discover_tf']
    path = f"{provider['protocol']}://{provider['host'].rstrip('/')}/{config['provider_path'].lstrip('/')}"

    return {'test': 'return'}


def handler(event, context):
    if run_cumulus_task:
        return run_cumulus_task(lambda_handler, event, context)
    else:
        return []
