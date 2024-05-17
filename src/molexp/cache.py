
import csv
import dbm


class JsonCache:
    def __init__(self, cache_path: str, name: str="json_cache"):
        self.cache_path = f"{cache_path}/{name}"

    def keys(self) -> list:
        with dbm.open(self.cache_path, "r") as db:
            return list(db.keys())

    def write(self, data: str, id_: str) -> None:
        print(data)
        with dbm.open(self.cache_path, "c") as db:
            db[id_] = data

    def read(self, id_: str) -> str:
        with dbm.open(self.cache_path, "r") as db:
            return db[id_].decode()

    def delete(self, id_: str) -> None:
        with dbm.open(self.cache_path, "r") as db:
            del db[id_]


class CsvCache:

    def __init__(self, cache_path: str, name: str="csv_cache"):
        self.cache_path = f"{cache_path}/{name}"

    def header(self) -> list:
        with open(self.cache_path, "r") as f:
            csv_reader = csv.reader(f)
            header = next(csv_reader)
        return header
    
    def write_dict(self, data: dict) -> None:
        with open(self.cache_path, "a") as f:

            csv_writer = csv.DictWriter(f, fieldnames=data.keys())
            csv_writer.writeheader()
            csv_writer.writerow(data)

            