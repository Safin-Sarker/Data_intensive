from pyspark.sql import SparkSession, column
import time

start_time = time.time()
spark = SparkSession.builder.appName("how to read csv file").getOrCreate()
data_frame = spark.read.csv(
    "hdfs://namenode:9000/output_mapper/output1/part-00000",
    header=True,
    inferSchema=True,
)

data_frame.printSchema()
def column_rename(data_frame):
     for col in data_frame.columns:
         col_new = col.replace('.','')
         data_frame = data_frame.withColumnRenamed(col, col_new)
     return data_frame
import re
from pyspark.sql.functions import col

data_frame_2 = column_rename(data_frame)
data_frame_2.columns
data_frame_3 = data_frame_2.select('FieldId', col('md').alias('id'), col('east').alias('id'),col('north').alias('id')))
data_frame_3.groupBy('FieldId').count().orderBy('count', ascending=False)

from pyspark.sql.functions import *
from pyspark.ml.feature import StringIndexer, IndexToString

indexing_string = StringIndexer(inputCol="id", outputCol="id_int")
indexing_model = indexing_string.fit(data_frame_3)
data_frame_5 = indexing_model.transform(data_frame_3)

data_frame_5.groupBy('id_int').count().orderBy('count', ascending=False)

indexing_string = StringIndexer(inputCol="FieldId", outputCol="FieldId")
indexing_string
indexing_model = indexing_string.fit(data_frame_5)
data_frame_6 = indexing_model.transform(data_frame_5)

data_frame_6.show(5)

train_dataset, testing_dataset = data_frame_6.randomSplit([0.75,0.25])
from pyspark.ml.recommendation import ALS
rs_transform = ALS(maxIter=10, regParam=0.01, userCol='FieldId', itemCol='id_int', nonnegative=True, coldStartStrategy="drop")
rs_transform = rs_transform.fit(train_dataset)
prediction = rs_transform.transform(testing_dataset)
data_out = zip(FieldId, predictions)
# Convert the list of tuples to a Spark RDD
rdd = sc.parallelize(data)
df = rdd.toDF(["FieldId", "prediction"])
predictions = df.coalesce(1)
predictions.write.mode("overwrite").option("header", "true").csv("path/to/output2")


from pyspark.ml.evaluation import RegressionEvaluator
evl = RegressionEvaluator(metricName='rmse', predictionCol='prediction')
rmse_value = evl.evaluate(prediction)

main_dataset = data_frame_6.select('id_int').distinct()
selected_dataset = data_frame_6.filter(data_frame_6['FieldId'] ).select('id_int').distinct()
selected_dataset = selected_dataset.withColumnRenamed("id_int", "id_int_used")
joined_dataset = main_dataset.join(selected_dataset, main_dataset.id_int == selected_dataset.id_int_used, how='left')

new_dataset = joined_dataset.where(col('id_int_used').isNull()).select(col('id_int')).distinct()
new_dataset = new_dataset.withColumn("FieldId",lit(int(76)))

recommondation = rs_transform.transform(new_dataset).orderBy('prediction', ascending=False)
recommondation.createTempView('recommondation')

recommondation_5 = spark.sql('SELECT id_int FROM recommondation limit 5')

selected_dataset_id = recommondation_5.join(data_frame_6, recommondation_5.id_int == data_frame_6.id_int, how='left')
# productTitle
selected_dataset_id.select('id').join(data_frame_2, selected_dataset_id.id == data_frame_2.timestamp, how='left').select('md').distinct().show(5)

end_time = time.time() - start_time
final_time = time.strftime("%H:%M:%S", time.gmtime(end_time))
