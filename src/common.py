import json
import os
from typing import Literal

from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient

BRONSYSTEEM_TO_EVENTHUB_NAME_MAPPING = {
    "anpr2": "anpr",
    "lvma2": "lvma-telcamera-v2",
    "lvma3": "vma-telcamera-v3",
    "lvma23mix": "lvma-peoplemeasurement",
    "reis1": "reistijden",
    "vlog1": "vlog",
}


def get_environment_name(
    method: Literal["env_variable", "cluster_tag"] = "env_variable"
) -> str:
    if method == "env_variable":
        return os.environ["DATABRICKS_OTAP_ENVIRONMENT"]
    else:
        raise ValueError(f"Unknown method '{method}', cannot get environment name...")


def get_key_vault_name(environment: str) -> str:
    if environment == "Ontwikkel":
        return "kv-dpmo-ont-01-fw3j"
    elif environment == "Productie":
        return "kv-dpmo-prd-01-Ef1e"
    else:
        raise ValueError(
            f"Unknown environment '{environment}, cannot determine key vault name."
        )


def retrieve_secret_from_vault(secret_name: str) -> str:
    key_vault_name = get_key_vault_name(environment=get_environment_name())
    key_vault_url = f"https://{key_vault_name}.vault.azure.net"
    credential = DefaultAzureCredential()
    client = SecretClient(vault_url=key_vault_url, credential=credential)
    return str(client.get_secret(secret_name).value)


def write_json(dir_path: str, filename: str, data_to_write: any):
    os.makedirs(dir_path, exist_ok=True)

    filepath = f"{dir_path}/{filename}"
    with open(filepath, mode="w", encoding="utf8") as output_file:
        json.dump(data_to_write, output_file)


def write_xml(dir_path: str, filename: str, data_to_write: str):
    os.makedirs(dir_path, exist_ok=True)

    filepath = f"{dir_path}/{filename}"
    with open(filepath, "w") as f:
        f.write(data_to_write)
