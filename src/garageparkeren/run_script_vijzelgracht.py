import asyncio

import nest_asyncio
from azure.identity.aio import DefaultAzureCredential

from src.common import (
    BRONSYSTEEM_TO_EVENTHUB_NAME_MAPPING,
    BRONSYSTEEM_TO_FILE_FORMAT_MAPPING,
    get_checkpoint_blob_storage_account_url,
    get_environment_name,
    retrieve_secret_from_vault,
)
from src.eventhub_to_sa import main
from src.settings import checkpoint_blob_container_name, consumer_group

nest_asyncio.apply()


if __name__ == "__main__":
    BRONSYSTEEM = "vijzelgracht"
    event_hub_name = BRONSYSTEEM_TO_EVENTHUB_NAME_MAPPING[BRONSYSTEEM]
    write_format = BRONSYSTEEM_TO_FILE_FORMAT_MAPPING[BRONSYSTEEM]

    print(f"STARTING!!! --- eventhub '{event_hub_name}' to landing zone '{BRONSYSTEEM}'-container ---")
    fully_qualified_namespace = retrieve_secret_from_vault(secret_name="eventhub-fully-qualified-namespace")
    credential = DefaultAzureCredential()

    # Run the main method.
    asyncio.run(
        main(
            credential=credential,
            blob_storage_account_url=get_checkpoint_blob_storage_account_url(environment=get_environment_name()),
            blob_container_name=checkpoint_blob_container_name,
            event_hub_fully_qualified_namespace=fully_qualified_namespace,
            event_hub_name=event_hub_name,
            consumer_group=consumer_group,
            write_format=write_format,
        )
    )
