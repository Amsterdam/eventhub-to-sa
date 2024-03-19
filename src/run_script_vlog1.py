import asyncio

import nest_asyncio
from azure.identity.aio import DefaultAzureCredential

from src.common import retrieve_secret_from_vault, BRONSYSTEEM_TO_EVENTHUB_NAME_MAPPING
from src.eventhub_to_sa import main
from src.settings import (
    checkpoint_blob_container_name,
    checkpoint_blob_storage_account_url,
)

# TODO evne uitzoeken waar we deze moeten aanroepen. Hier, in eventhub_to_sa.py of beide
nest_asyncio.apply()


if __name__ == "__main__":
    bronsysteem = "vlog1"
    print(f"STARTING!!! --- eventhub-to-landingzone-{bronsysteem}")
    fully_qualified_namespace = retrieve_secret_from_vault(
        secret_name="eventhub-fully-qualified-namespace"
    )
    event_hub_name = BRONSYSTEEM_TO_EVENTHUB_NAME_MAPPING[bronsysteem]
    consumer_group = "test"
    credential = DefaultAzureCredential()

    # Run the main method.
    asyncio.run(
        main(
            credential=credential,
            blob_storage_account_url=checkpoint_blob_storage_account_url,
            blob_container_name=checkpoint_blob_container_name,
            event_hub_fully_qualified_namespace=fully_qualified_namespace,
            event_hub_name=event_hub_name,
            consumer_group=consumer_group,
            write_format="xml",
        )
    )
