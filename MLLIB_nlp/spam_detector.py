import findspark
findspark.init()

from pyspark.sql import SparkSession
from pyspark.sql.functions import length
from pyspark.ml.feature import Tokenizer,StopWordsRemover, CountVectorizer,IDF,StringIndexer
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.linalg import Vector
from pyspark.ml.classification import NaiveBayes
from pyspark.ml import Pipeline
from pyspark.ml.evaluation import MulticlassClassificationEvaluator

spark = SparkSession.builder.appName('nlp').getOrCreate()
data = spark.read.csv("smsspamcollection/SMSSpamCollection",inferSchema=True,sep='\t')
data = data.withColumnRenamed('_c0','class').withColumnRenamed('_c1','text')
data = data.withColumn('length',length(data['text']))

ham_spam_to_num = StringIndexer(inputCol='class',outputCol='label')
tokenizer = Tokenizer(inputCol="text", outputCol="token_text")
stopremove = StopWordsRemover(inputCol='token_text',outputCol='stop_tokens')
count_vec = CountVectorizer(inputCol='stop_tokens',outputCol='c_vec')
idf = IDF(inputCol="c_vec", outputCol="tf_idf")
clean_up = VectorAssembler(inputCols=['tf_idf','length'],outputCol='features')

data_prep_pipe = Pipeline(stages=[ham_spam_to_num, tokenizer, stopremove, count_vec, idf, clean_up])

cleaner = data_prep_pipe.fit(data)
clean_data = cleaner.transform(data)
clean_data = clean_data.select(['label','features'])

(training,testing) = clean_data.randomSplit([0.7,0.3])

nb = NaiveBayes()
spam_predictor = nb.fit(training)
test_results = spam_predictor.transform(testing)

acc_eval = MulticlassClassificationEvaluator()
acc = acc_eval.evaluate(test_results)
print("Accuracy of model at predicting spam was: {}".format(acc))
