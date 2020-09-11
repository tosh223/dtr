import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
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

    def write(self, df: pd.DataFrame, key: str) -> None:
        pandavro.to_avro(key, df, append=True)

class ORC():
    def __init__(self):
        pass

    def read(self, key: str) -> pd.DataFrame:
        pass
    
    def write(self, df: pd.DataFrame, key: str) -> None:
        pass
