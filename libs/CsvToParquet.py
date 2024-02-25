import pandas as pd

path = '/datasets/'
file = 'v2.csv'

class CsvToParquet:

    def __init__(self):
        self.pathOut = r'C:\Users\rafinha\workspace\new_repo\S3\parquet'

    def converte(self, caminho, arquivo):

        filename = file[:-4]
        pathOut = filename + '.parquet'
        df = pd.read_csv(caminho + arquivo)
        df.to_parquet(self.pathOut + pathOut)


a = CsvToParquet()
a.converte(path, file)
