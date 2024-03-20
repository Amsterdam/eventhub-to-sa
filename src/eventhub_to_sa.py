from datetime import datetime
from typing import Literal

import nest_asyncio
from azure.eventhub import EventData
from azure.eventhub.aio import EventHubConsumerClient, PartitionContext
from azure.eventhub.extensions.checkpointstoreblobaio import BlobCheckpointStore
from azure.identity.aio import DefaultAzureCredential
from src.common import write_json

nest_asyncio.apply()

CACHE = {}
MINUTES_BEFORE_FLUSHING_TO_SA = 1  # Adjust to 15 minutes? before running on prod
START_SCRIPT_DATE_TIME = datetime.now()


async def on_event_batch_xml(
    partition_context: PartitionContext, event_batch: list[EventData]
) -> None:
    on_event_batch_date_time = datetime.now()
    print(
        f"Received event from partition: {partition_context.partition_id}. {len(event_batch)}"
    )
    if partition_context.partition_id not in CACHE:
        print("not in cache")
        CACHE[partition_context.partition_id] = {}
        CACHE[partition_context.partition_id][
            "last_flush_datetime"
        ] = START_SCRIPT_DATE_TIME
        CACHE[partition_context.partition_id]["cached_events"] = event_batch
    else:
        print("in cache")
        CACHE[partition_context.partition_id]["cached_events"].extend(event_batch)
        print(len(CACHE[partition_context.partition_id]["cached_events"]))

    if (
        on_event_batch_date_time
        - CACHE[partition_context.partition_id]["last_flush_datetime"]
    ).seconds > MINUTES_BEFORE_FLUSHING_TO_SA * 60:
        print("!!!!flush to storage account and updateoffset!!!!")
        # TODO implement store data common.py write_xml
        data = list(map(lambda e: e.body_as_str(), event_batch))

        if len(CACHE[partition_context.partition_id]["cached_events"]) > 0:
            await partition_context.update_checkpoint(
                CACHE[partition_context.partition_id]["cached_events"][-1]
            )
        CACHE[partition_context.partition_id]["cached_events"] = []
        CACHE[partition_context.partition_id][
            "last_flush_datetime"
        ] = on_event_batch_date_time
    else:
        print("min wait time not met")


async def on_event_batch_json(
    partition_context: PartitionContext, event_batch: list[EventData]
) -> None:
    on_event_batch_date_time = datetime.now()
    print(
        f"Received event from partition: {partition_context.partition_id}. {len(event_batch)}"
    )
    if partition_context.partition_id not in CACHE:
        print("not in cache")
        CACHE[partition_context.partition_id] = {}
        CACHE[partition_context.partition_id][
            "last_flush_datetime"
        ] = START_SCRIPT_DATE_TIME
        CACHE[partition_context.partition_id]["cached_events"] = event_batch
    else:
        print("in cache")
        CACHE[partition_context.partition_id]["cached_events"].extend(event_batch)
        print(len(CACHE[partition_context.partition_id]["cached_events"]))

    if (
        on_event_batch_date_time
        - CACHE[partition_context.partition_id]["last_flush_datetime"]
    ).seconds > MINUTES_BEFORE_FLUSHING_TO_SA * 60:
        print("!!!!flush to storage account and updateoffset!!!!")
        # TODO implement store data common.py write_json
        data_to_write = list(s.body_as_str() for s in event_batch)
        filename = f'{CACHE[partition_context.partition_id]["last_flush_datetime"]}_{on_event_batch_date_time}_{partition_context.partition_id}.json'
        write_json(
            dir_path="/Volumes/dpmo_dev/default/landingzone/vlog/v1/",
            filename=filename,
            data_to_write=data_to_write
        )

        if len(CACHE[partition_context.partition_id]["cached_events"]) > 0:
            await partition_context.update_checkpoint(
                CACHE[partition_context.partition_id]["cached_events"][-1]
            )
        CACHE[partition_context.partition_id]["cached_events"] = []
        CACHE[partition_context.partition_id][
            "last_flush_datetime"
        ] = on_event_batch_date_time
    else:
        print("min wait time not met")


# TODO on_event_batch meegeven als param callable
async def main(
    credential: DefaultAzureCredential,
    blob_storage_account_url: str,
    blob_container_name: str,
    event_hub_fully_qualified_namespace: str,
    event_hub_name: str,
    consumer_group: str,
    write_format: Literal["json", "xml"],
) -> None:
    # Create an Azure blob checkpoint store to store the checkpoints.
    checkpoint_store = BlobCheckpointStore(
        blob_account_url=blob_storage_account_url,
        container_name=blob_container_name,
        credential=credential,
    )

    # Create a consumer client for the event hub.
    client = EventHubConsumerClient(
        fully_qualified_namespace=event_hub_fully_qualified_namespace,
        eventhub_name=event_hub_name,
        consumer_group=consumer_group,
        checkpoint_store=checkpoint_store,
        credential=credential,
    )

    if write_format == "json":
        on_batch = on_event_batch_json
    elif write_format == "xml":
        on_batch = on_event_batch_xml
    else:
        raise ValueError(f"Unknown 'write_format' value, '{write_format}'.")

    async with client:
        await client.receive_batch(  # Replace with client.receive_batch()
            on_event_batch=on_batch,
            max_wait_time=1,
            starting_position="-1",  # "-1" is from the beginning of the partition.
            # prefetch=2,
            max_batch_size=1000,
        )

    # Close credential when no longer needed.
    await credential.close()
