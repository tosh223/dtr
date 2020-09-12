import os
import pandas as pd
import src.dtr as dtr

current_dir = os.path.dirname(os.path.abspath(__file__))
csv_file = f'{current_dir}/data/test.csv'
pq_file = csv_file.replace('.csv', '.parquet')
avro_file = csv_file.replace('.csv', '.avro')
avsc_file = csv_file.replace('.csv', '.avsc')

# @profile
def main():

    # csv
    csv = dtr.Csv()
    df_csv = csv.read(key=csv_file)
    print(df_csv.head())

    # parquet
    parquet = dtr.Parquet()
    parquet.write(df=df_csv, key=pq_file)
    df_pq = parquet.read(key=pq_file)
    print(df_pq.head())

    # Avro
    avro = dtr.Avro()
    avro.write(df=df_csv, schema_key=avsc_file, file_key=avro_file)
    df_avro = avro.read(key=avro_file)
    print(df_avro.head())

###############################################
if __name__ == '__main__':
    main()