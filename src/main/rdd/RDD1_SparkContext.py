from pyspark import SparkContext, SparkConf

conf = ((((SparkConf()
           .setAppName("SparkContextExample1")
           .setMaster("local[2]")
           .set("spark.executor.memory", "2g"))
          .set("spark.driver.memory", "1g"))
         .set("spark.executor.cores", "4"))
        .set("spark.executor.instances", "2"))

# Creating Spark Context 1st way
sc1 = SparkContext(conf=conf)
print("ApplicationName: ", sc1.appName)
print("Master: ", sc1.master)
'''
ApplicationName:  SparkContextExample1
Master:  local[2]
'''

#Creating Spark Context 2nd way
# sc2 = SparkContext("local[*]", "SparkContext_Example")
#
# print("ApplicationName: ", sc2.appName)
# print("Master:", sc2.master)
'''
ApplicationName:  SparkContext_Example
Master: local[*]
'''

'''
If we create multiple spark context in single program, then will get below error
ValueError: Cannot run multiple SparkContexts at once; existing SparkContext(app=SparkContextExample1, master=local[2]) created by __init__ at
'''