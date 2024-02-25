import os
from pyspark import SparkContext
from pyspark import SQLContext
os.environ['SPARK_HOME'] = 'C:\spark-2.3.4-bin-hadoop2.6'

sc = SparkContext('local', 'First teste')
spark = SQLContext(sc)

servico = "controlededados"

listparams = ["ptabelacredito", "ptabelapd", "ptabelaxpto"]

columns = ["servico","parametro","valor"]
data = [("controlededados", "ptabelacredito", "tbkm3_credito"), ("controlededados", "ptabelapd", "tbkm3_pd"), ("controlededados", "ptabelaxpto", "tbkm3_xpto"),
        ("marcacaodecarteira", "ptabelacredito", "tbkm3_credito"), ("marcacaodecarteira", "ptabelapd", "tbkm3_pd"), ("marcacaodecarteira", "ptabelaxpto", "tbkm3_xpto")]

df = spark.createDataFrame(data,schema=columns)
df.show()
dffiltrado = df.select(df.parametro, df.valor).where(df.servico == "controlededados")
#df.filter(df.servico == "controlededados")
dffiltrado.show()
tableparams = "km3.tbkm3_params"
connectorRds = "Mysql connection"
servico = "contolededados"
pathQuery = ""
glueContext = ''

query = sc.textFile(pathQuery)


class ObterParams:

        def __init__(self, sc, glueContext, connectorRds, tableParams):

            self.sc = sc
            self.glueContext = glueContext
            self.connectorRds = connectorRds
            self.tableParams = tableParams

        def QueryT (self, servico, query):

                dynfparams = self.glueContext.create_dynamic_frame.from_options(
                        connection_type="mysql",
                        connection_options={
                                "useConnectionProperties": "true",
                                "dbtable": f"{self.tableParams}",
                                "connectionName": f"{self.connectorRds}",
                        }
                )

                dfparams = dynfparams.toDF()
                dfparams = dfparams.select(dfparams.parametro, dfparams.valor).where(dfparams.servico == f"{servico}")

                for x in dfparams.collect():
                        query.replace(x[0], x[1])

                return  query


        def Jobs (self, servico, listparams):

                dynfparams = self.glueContext.create_dynamic_frame.from_options(
                        connection_type="mysql",
                        connection_options={
                                "useConnectionProperties": "true",
                                "dbtable": f"{self.tableParams}",
                                "connectionName": f"{self.connectorRds}",
                        }
                )

                dfparams = dynfparams.toDF()
                dfparams = dfparams.select(dfparams.parametro, dfparams.valor).where(dfparams.servico == f"{servico}")

                for x in listparams:
                        i = 0
                        for y in dffiltrado.collect():
                                parm = ""
                                x = y['{x}']
                                print(x)


a = ObterParams(sc, glueContext, connectorRds, tableparams)
a.QueryT(servico=servico, query=query)