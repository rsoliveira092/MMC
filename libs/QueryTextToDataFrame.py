import os
import re
from pyspark import SparkContext
from pyspark import SQLContext
import pandas as pd

os.environ['SPARK_HOME'] = 'C:\spark-2.3.4-bin-hadoop2.6'
path = r'/S3/03. queries/marcaSegmentos.sql'

sc = SparkContext('local', 'First teste')
spark = SQLContext(sc)

contratostmp = spark.read.options(header='True', delimiter=',').csv(r'C:\Users\rafinha\Documents\contratos.csv')
contratostmp.createOrReplaceTempView("CONTRATOS")
segmentostmp = spark.read.options(header='True', delimiter=',').csv(r'C:\Users\rafinha\Documents\segmentos.csv')
segmentostmp.createOrReplaceTempView("SEGMENTOS")


class QuerytoPySpark:
    def formataQuery(self):
        if type(path) == str:

            return sc.textFile(path).reduce(lambda x, y: re.sub(" +"," ",str(x + ' ' + y)).replace("\n","")).split(";")
        else:
            return sc.textFile(path).reduce(lambda x, y: re.sub(" +"," ",str(x + ' ' + y)).replace("\n","")).split(";")

    def executaQuery (self):

        listaTabelasTemp = []
        i = 0

        for query in self.formataQuery():
            if len(query) > 6:    #Nao executa select para espacos
                print (query)
                listaTabelasTemp.append(spark.sql(query))
                listaTabelasTemp[i].createOrReplaceTempView("tabelaTemp_{n}".format(n=i))

                listaTabelasTemp[i].show()

                i = i+1

        listaTabelasTemp[-1].write.mode('overwrite').parquet(r'C:\Users\rafinha\Documents\parquet')


a = QuerytoPySpark()
print(a.executaQuery())