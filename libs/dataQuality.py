import os
from pyspark import SparkContext
from pyspark import SQLContext
import json

os.environ['SPARK_HOME'] = 'C:\spark-2.3.4-bin-hadoop2.6'

sc = SparkContext('local', 'First teste')
spark = SQLContext(sc)

contratostmp = spark.read.options(header='True', delimiter=',').csv(r'C:\Users\rafinha\Documents\contratos.csv')
controletmp = spark.read.options(header='True', delimiter=',').csv(r'C:\Users\rafinha\workspace\new_repo\datasets'
                                                                   r'\tb_controle_entradas.csv')

contratostmp.createOrReplaceTempView("tb_contrato_credito")
controletmp.createOrReplaceTempView("tb_controle_entradas")

df = spark.sql("SELECT * FROM tb_contrato_credito")
df.show()

pathDataqualityDict = r"C:\Users\rafinha\workspace\new_repo\04. dicionarios\dic-dataquality"
pathQueryNull = r'C:\Users\rafinha\workspace\new_repo\S3\queries\nulos.sql'
pathQueryDupl = r'C:\Users\rafinha\workspace\new_repo\S3\queries\duplicados.sql'

with open(pathDataqualityDict) as f:
    DataqualityDict = f.read()

with open(pathQueryNull) as f:
    QueryNull = f.read()

with open(pathQueryDupl) as f:
    QueryDupl = f.read()

js = json.loads(DataqualityDict)


class Quality:

    def campos(self, tabela=None, campo=None):

        a = js[tabela][campo]
        print (a)
        i = 0

        for x in a:
            print(a[i])
            self.validaCampos(tabela, campo, a[i])
            i = i + 1

    def validaCampos(self, tabela, campo, validador):

        if validador == 'nulos':
            dfresult = spark.sql(QueryNull.format(Vcampo=campo, Vtabela=tabela))
            dfresult.show()

        elif validador == 'duplicados':
            dfresult = spark.sql(QueryDupl.format(Vcampo=campo, Vtabela=tabela))
            dfresult.show()

        else:
            print ("OUTRA VALIDACAO")


a = Quality()
a.campos('tb_contrato_credito', "contrato")
