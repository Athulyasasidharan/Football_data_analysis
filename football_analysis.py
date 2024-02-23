# --project1: uefa champions football data analysis
# step1: create a folder in hdfs and name it as spark project,load your uefa data in to spark project
# step1:create a dataframe and load data to dataframe print column name
# no of rows and no of rows and data type in an each column


from pyspark.sql import SparkSession
spark=SparkSession.builder.appName('demo').getOrCreate()
# uefa=spark.read.options(header=True,inferSchema=True).csv('hdfs://localhost:9000/sparkproject1')
# uefa.show()
#
# # --to print column names
# print('column names are:')
# for i in uefa.columns:
#     print(i)
#
# # --to print no.columns
#
# print('no of columns are:',len(uefa.columns))
#
# # --to print no of rows
#
# print('no of rows are:',uefa.count())
#
# # analysis 1: draw a graph of away team and home team goal scoring in each year of quarterfinal,semifinal and final.(plot it as 2 graph)
#
# from pyspark.sql import functions as f
# newuefa=uefa.withColumn("newdate",f.from_unixtime(f.unix_timestamp(uefa.date),"yyyy-MM-dd"))
# newuefa.show()
# newuefa.printSchema()
#
# def yeargenerator(data):
#     li=data.split('-')
#     return li[0]
#
# myfn=f.udf(yeargenerator)
#
# out=newuefa.withColumn('year',myfn(newuefa['newdate']))
# out.show()
#
#
# flt_out=out.filter((out['round']=='round : quarterfinals') | (out['round']=='round : semifinals') | (out['round']=='round : finals'))
# new= flt_out.select('homescore','awayscore','round','year')
# new.show(n=50)
# new.printSchema()
#
# def myremove(value):
#         return value[0]
#
# newfn=f.udf(myremove)
# one=new.withColumn('home_score',newfn(new['homescore']))
# result=one.withColumn('away_score',newfn(one['awayscore']))
# final= result.drop('homescore','awayscore')
# final.show(n=50)
# final.printSchema()
#
#
# from pyspark.sql.types import IntegerType
# newdf=final.withColumn('hmscore',final['home_score'].cast(IntegerType()))
# newdf.show()
# newdf.printSchema()
#
#
#
# newdf=final.withColumn('hmscore',final['home_score'].cast(IntegerType()))
# finaldf=newdf.withColumn('awscore',newdf['away_score'].cast(IntegerType()))
# finaldf=finaldf.drop('home_score','away_score')
# finaldf.show()
# finaldf.printSchema()
#
#
# import pyspark.sql.functions as f
# grp=finaldf.groupby('year').agg(f.sum('hmscore').alias('totalhomegoals'),f.sum('awscore').alias('totalawaygoals'))
# grp.show()
# grp=grp.orderBy('year')
# import pandas as pd
# df=grp.toPandas()
# import matplotlib.pyplot as plt
# plt.plot(df['year'],df['totalhomegoals'])
# plt.plot(df['year'],df['totalawaygoals'])
# plt.show()




# analysis 2 - teams that most appeared in quarterfinal,semifinal,final

uefa=spark.read.csv('hdfs://localhost:9000/sparkproject1/UEFAChampionsLeague2004-2021.csv',header=True,inferSchema=True)
uefa.show()

newuefa= uefa.filter((uefa['round']=='round : quarterfinals') | (uefa['round']=='round : semifinals') | (uefa['round']=='round : final'))
newuefa.show()
#
qf=uefa.filter(uefa['round']=='round : quarterfinals')
sf=uefa.filter(uefa['round']=='round : semifinals')
fi=uefa.filter(uefa['round']=='round : final')

qf.show()
sf.show()
fi.show()



qf=qf.select('_c0','homeTeam','round','date')
sf=sf.select('_c0','homeTeam','round','date')
fi=fi.select('_c0','homeTeam','round','date')

qf.show()
sf.show()
fi.show()

import pyspark.sql.functions as f

li=[qf,sf,fi]
for i in li:
    out1= i.groupBy('homeTeam').agg(f.count('_c0').alias('no_of_participation'))
    out2= out1.orderBy('no_of_participation',ascending=False)
    out2.show()
    maxvalue=out2.select(f.max(out2.no_of_participation))
    print(maxvalue.collect()[0])
    print('..................................................')


