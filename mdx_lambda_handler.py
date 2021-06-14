import os
import sys

from process_mdx.main import MDX

run_cumulus_task = None
if os.environ.get('CUMULUS_MESSAGE_ADAPTER_DIR'):
    sys.path.insert(0, os.environ.get('CUMULUS_MESSAGE_ADAPTER_DIR'))
    from run_cumulus_task import run_cumulus_task


def mdx_lambda_handler(event, context=None):
    print(event)
    MDX.cli()
    return {"Temp": "temp"}


def handler(event, context):
    if run_cumulus_task:
        return run_cumulus_task(mdx_lambda_handler, event, context)
    else:
        return []
