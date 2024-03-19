import asyncio
import nest_asyncio
from src.eventhub_to_sa import main, on_event_batch_xml
from azure.identity.aio import DefaultAzureCredential

# TODO evne uitzoeken waar we deze moeten aanroepen. Hier, in eventhub_to_sa.py of beide
nest_asyncio.apply()


if __name__ == "__main__":
    # TODO we kunnen deze misschien voor het gemak in een soort settings.py zetten
    blob_storage_account_url = "https://checkpointweuovbiesondow.blob.core.windows.net"
    blob_container_name = "event-hub-checkpoints"
    # hardTODO Deze misschien handig in de keyvault dan kan je hem voor dev en prod zonder trucjes gebruiken
    event_hub_fully_qualified_namespace = "dpmo-weu-eventhub-namespace-o-vbiesondow2u.servicebus.windows.net"
    event_hub_name = 'vlog'
    credential = DefaultAzureCredential()
    # Run the main method.
    asyncio.run(main(credential, blob_storage_account_url, blob_container_name, event_hub_fully_qualified_namespace,
                     event_hub_name, on_event_batch_xml))
