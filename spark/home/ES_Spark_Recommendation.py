ELASTICSEARCH_IP = "172.25.0.3"

from pyspark.sql import *
from pyspark.sql.functions import *
spark = SparkSession.builder \
    .appName("recommendation_system") \
    .config("spark.es.nodes.wan.only","true") \
    .config("spark.es.nodes",ELASTICSEARCH_IP) \
    .config("spark.es.port","9200") \
    .getOrCreate()
spark


# In[2]:


import gzip
import json
from pyspark.sql.types import *


def parse(path):
    g = gzip.open(path, 'rb')
    for l in g:
        yield json.loads(l)

        
def getMetaData(path):
    data = []
    data_schema =  [
                       StructField("asin", StringType(), True),
                       StructField("title", StringType(), True),
                       StructField("brand", StringType(), True),
                       StructField("category", ArrayType(StringType(), True), True),
                       StructField("main_category", StringType(), True),
                       StructField("image", ArrayType(StringType(), True), True)
                   ]
    final_schema = StructType(fields=data_schema)
    for d in parse(path):
        try:
            review = {}
            review['asin'] = d['asin']
            review['title'] = d['title']
            review['brand'] = d['brand']
            review['category'] = d['category']
    #         print(d['category'])
            review['main_category'] = next(reversed(d['category']), None) if len(d['category'])!= 0 else ''
            review['image'] = d['imageURL']
            data.append(review)
        except:
           print('error')

#   print(df)
    return spark.createDataFrame(data, schema=final_schema)

product_data = getMetaData('./data/meta_Appliances.json.gz')
product_data = product_data.dropDuplicates(['asin'])
product_data.limit(1).toPandas()
product_data.printSchema()


# In[3]:


product_data.groupBy("main_category").count().orderBy(col('count').desc()).show(100)


# In[4]:


from elasticsearch import Elasticsearch

# test your ES instance is running
es = Elasticsearch(f'http://{ELASTICSEARCH_IP}:9200')
es.info(pretty=True)


# In[5]:


if es.indices.exists(index="products"):
    es.indices.delete(index="products")
VECTOR_DIM = 25

product_mapping = {
    # this mapping definition sets up the metadata fields for the products
    "properties": {
        "asin": {
            "type": "keyword"
        },
        "title": {
            "type": "keyword"
        },
        "image": {
            "type": "keyword"
        },
        "brand": {
            "type": "keyword"
        },
        "category": {
            "type": "keyword"
        },
        "main_category": {
            "type": "keyword"
        },
        # the following fields define our model factor vectors and metadata
        "model_factor": {
            "type": "dense_vector",
            "dims" : VECTOR_DIM
        },
        "model_version": {
            "type": "keyword"
        },
        "model_timestamp": {
            "type": "date"
        }          
    }
}

res_products = es.indices.create(index="products", mappings=product_mapping)

print("Created indices:")
print(res_products)


# In[6]:


es.count(index="products")['count']


# In[7]:


product_data.write.format("es").option("es.mapping.id", "asin").save("products")
num_products_df = product_data.count()
num_products_es = es.count(index="products")['count']
# check load went ok
print("Product DF count: {}".format(num_products_df))
print("ES index count: {}".format(num_products_es))


# In[8]:


es.search(index="products", q="main_category:Refrigerators", size=3)


# In[9]:


def getRatingData(path):
    data = []
    data_schema = [
               StructField("asin", StringType(), True),
               StructField("reviewerId", StringType(), True),
               StructField("rating", FloatType(), True)]
    final_schema = StructType(fields=data_schema)
    for d in parse(path):
        review = {}
        review['asin'] = d['asin']
        review['reviewerId'] = d['reviewerID']
        review['rating'] = d['overall']
        data.append(review)
#   print(df)
    return spark.createDataFrame(data, schema=final_schema)

df_rating= getRatingData('./data/Appliances.json.gz')
df_rating.limit(3).toPandas()


# In[10]:


from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.recommendation import ALS

from pyspark.ml.feature import StringIndexer
from pyspark.ml import Pipeline
from pyspark.sql.functions import col

indexer = [StringIndexer(inputCol=column, outputCol=column+"_index") for column in list(set(df_rating.columns)-set(['rating'])) ]
pipeline = Pipeline(stages=indexer)
transformed = pipeline.fit(df_rating).transform(df_rating)
transformed.show()


# In[11]:


als=ALS(maxIter=5,regParam=0.09,rank=25,userCol="reviewerId_index",itemCol="asin_index",ratingCol="rating",coldStartStrategy="drop",nonnegative=True)
model=als.fit(transformed)


# In[12]:


model.itemFactors.count()


# In[13]:


evaluator=RegressionEvaluator(metricName="rmse",labelCol="rating",predictionCol="prediction")
predictions=model.transform(transformed)
rmse=evaluator.evaluate(predictions)
print("RMSE="+str(rmse))
predictions.show()


# In[14]:


from pyspark.sql.functions import lit, current_timestamp, unix_timestamp
ver = model.uid
ts = unix_timestamp(current_timestamp())
product_vectors = model.itemFactors.select("id",\
                                         col("features").alias("model_factor"),\
                                         lit(ver).alias("model_version"),\
                                         ts.alias("model_timestamp"))
product_vectors.show(2)


# In[15]:


asin_index_meta = [
    f.metadata for f in transformed.schema.fields if f.name == "asin_index"]
asin_index_labels = asin_index_meta[0]["ml_attr"]["vals"]

from pyspark.ml.feature import IndexToString

reviewerId_converter = IndexToString(inputCol="id", outputCol="asin",   labels=asin_index_labels)
PredictedLabels = reviewerId_converter.transform(product_vectors)
PredictedLabels = PredictedLabels.drop('id')
PredictedLabels.show(10)


# In[16]:


PredictedLabels.count()


# In[17]:


PredictedLabels.write.format("es") \
    .option("es.mapping.id", "asin") \
    .option("es.write.operation", "upsert") \
    .save("products", mode="append")

