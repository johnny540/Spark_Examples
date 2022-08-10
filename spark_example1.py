from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('Spark Example 1').getOrCreate()

list1 = [['Ram', 30], ['Anil', 28]]
df = spark.createDataFrame(list1, ['name', 'age'])
# df.show()
# spark.stop()
# LTI interview questions
df1 = spark.createDataFrame([[3],[3],[3],[3]],['id'])
df2 = spark.createDataFrame([[3],[None],[3]],['id'])
# df1.show()
# df2.show()
df1.join(df2,['id'],'left').show(50,False)
# Result
"""
+---+
|id |
+---+
|3  |
|3  |
|3  |
|3  |
|3  |
|3  |
|3  |
|3  |
+---+


Process finished with exit code 0

"""
# Added Comment
