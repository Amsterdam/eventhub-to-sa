import asyncio
import nest_asyncio
from src.eventhub_to_sa import main, on_event_batch_xml
from src.settings import (
    checkpoint_blob_container_name,
    checkpoint_blob_storage_account_url,
)
from src.common import retrieve_secret_from_vault
from azure.identity.aio import DefaultAzureCredential

# TODO evne uitzoeken waar we deze moeten aanroepen. Hier, in eventhub_to_sa.py of beide
nest_asyncio.apply()


if __name__ == "__main__":
    print("STARTING!!!")
    fully_qualified_namespace = retrieve_secret_from_vault(
        secret_name="eventhub-fully-qualified-namespace"
    )
    event_hub_name = "vlog"
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
            on_batch=on_event_batch_xml,
        )
    )
