import csv
import json
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import fastavro
import pandavro

class Csv():
    def __init__(self):
        pass

    def read(self, key: str) -> pd.DataFrame:
        df = pd.read_csv(key, keep_default_na=False)
        return df

    def write(self, df: pd.DataFrame, key: str) -> None:
        df.to_csv(key)

class Parquet():
    def __init__(self):
        pass

    def read(self, key: str) -> pd.DataFrame:
        df = pd.read_parquet(key)
        return df

    def write(self, df: pd.DataFrame, key: str) -> None:
        table = pa.Table.from_pandas(df)
        pq.write_table(table, key)

class Avro():
    def __init__(self):
        pass

    def read(self, key: str) -> pd.DataFrame:
        df = pandavro.read_avro(key)
        return df

    # def write(self, df: pd.DataFrame, schema_key: str, file_key: str) -> None:
    #     schema = self.__parse_schema(path=schema_key)
    #     records = df.to_dict(orient='records')
    #     with open(file_key, 'wb') as f:
    #         fastavro.writer(
    #             f,
    #             schema,
    #             records,
    #             # sync_interval=16000,
    #             codec='snappy'
    #         )

    def write(self, df: pd.DataFrame, schema_key: str, file_key: str) -> None:
        schema = self.__parse_schema(path=schema_key)
        pandavro.to_avro(file_key, df, schema=schema, append=False, codec='snappy')

    def __parse_schema(self,  path: str):
        with open(path, 'r') as f:
            return fastavro.parse_schema(json.load(f))

class ORC():
    def __init__(self):
        pass

    def read(self, key: str) -> pd.DataFrame:
        pass
    
    def write(self, df: pd.DataFrame, key: str) -> None:
        pass
