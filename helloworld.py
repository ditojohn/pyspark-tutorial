# Reference: https://www.youtube.com/watch?v=_C8kWso4ne4&t=618s

# 1.Intro
##############################################

import pyspark
from pyspark.sql import SparkSession
from pyspark.ml.feature import Imputer

spark = SparkSession.builder.appName('helloWorld').getOrCreate()
print(spark.sparkContext)

#df = spark.read.option('header', 'true').csv('data.csv', inferSchema=True)
df = spark.read.csv('data.csv', header=True, inferSchema=True)

# 2.Dataframes
##############################################
'''
df.printSchema()
print(df.columns)
print(df.dtypes)

df.show()
print(df.head(2))
df.select('Name').show()
df.select('Name', 'Experience').show()
df.describe().show()

df.withColumn('Experience After 2 Years', df['Experience']+2).show()
df.drop('Experience').show()
df.withColumnRenamed('Experience', 'Exp').show()
'''

# 3.Handle Missing Values
##############################################
'''
df.na.drop().show()
df.na.drop(how='all').show()
df.na.drop(how='any', thresh=2).show()
df.na.drop(how='any', subset=['Experience']).show()

df.na.fill(value='Missing').show()
df.na.fill(value=-1).show()

imputeCols = ['Age', 'Experience', 'Salary']
imputer = Imputer(
    inputCols=imputeCols,
    outputCols=["Imputed{}".format(col) for col in imputeCols]
).setStrategy("mean")
imputer.fit(df).transform(df).show()
'''

# 4.Filter Operations
##############################################
'''
df.filter("Salary<=35000").show()
df.filter("Salary<=35000").select(['Name', 'Age']).show()
df.filter((df['Salary']<=35000) & (df['Age']>30)).show()
df.filter(~(df['Salary']<=35000)).show()
'''

# 5.Group By and Aggregation
##############################################
'''
df.groupBy('Dept').sum().show()
df.groupBy('Dept').max('Salary').show()
df.agg({'Salary':'sum'}).show()
'''

# 6.ML DataFrame API
##############################################

from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression

training = spark.read.csv('training.csv', header=True, inferSchema=True)
featureAssembler = VectorAssembler(inputCols=["Age", "Experience"], outputCol="Independent Features")
output=featureAssembler.transform(training)
output.show()

finalized_data=output.select("Independent Features","Salary")
finalized_data.show()

##train test split
train_data,test_data=finalized_data.randomSplit([0.6,0.4])
regressor=LinearRegression(featuresCol='Independent Features', labelCol='Salary')
regressor=regressor.fit(train_data)
print(regressor.coefficients)
print(regressor.intercept)

pred_results=regressor.evaluate(test_data)
pred_results.predictions.show()

print(pred_results.meanAbsoluteError, pred_results.meanSquaredError)
