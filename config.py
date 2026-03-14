import os

from infrastructure.rabbitmq import config

STORAGE_ROOT_FOLDER = os.environ['STORAGE_ROOT_FOLDER']

action_handler = config.action_handler
run_action = config.run_action

app = config.create_faststream_app()