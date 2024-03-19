import json
import os


def write_json(dir_path: str, filename: str, data_to_write: any):
    os.makedirs(dir_path, exist_ok=True)

    filepath = f"{dir_path}/{filename}"
    with open(filepath, mode="w", encoding="utf8") as output_file:
        json.dump(data_to_write, output_file)


def xml(dir_path: str, filename: str, data_to_write: str):
    os.makedirs(dir_path, exist_ok=True)

    filepath = f"{dir_path}/{filename}"
    with open(filepath, "w") as f:
        f.write(data_to_write)
