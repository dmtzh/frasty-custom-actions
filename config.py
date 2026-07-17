import os

from actions.filternewdata.previousdatastore import PreviousDataStore
from actions.sendtoviberchannel.config import ViberApiConfig
from infrastructure.rabbitmq import config

STORAGE_ROOT_FOLDER = os.environ['STORAGE_ROOT_FOLDER']

_viber_api_config = ViberApiConfig.parse(os.environ["VIBER_API_URL"], os.environ["VIBER_API_HTTP_METHOD"])
if _viber_api_config is None:
    raise ValueError("Invalid Viber API configuration")
viber_api_config = _viber_api_config

action_handler = config.action_handler
run_action = config.run_action

previous_data_storage = PreviousDataStore(STORAGE_ROOT_FOLDER)

app = config.create_faststream_app()