from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.evaluation import BinaryClassificationEvaluator

spark = SparkSession.builder.appName('logreg_project').getOrCreate()
data = spark.read.csv('customer_churn.csv',inferSchema=True, header=True)


# Format for MLLIB

assembler = VectorAssembler(inputCols=['Age','Total_Purchase','Account_Manager','Years','Num_Sites'], outputCol='features')
output = assembler.transform(data)
final_data = output.select('features','churn') # 2 columns

# Test Train Split
train_churn,test_churn = final_data.randomSplit([0.7,0.3])

# Fit the model
lr_churn = LogisticRegression(labelCol='churn')
fitted_churn_model = lr_churn.fit(train_churn)

training_sum = fitted_churn_model.summary  # training summary DF
print(training_sum.predictions.describe().show())

# Evaluate results
pred_and_labels = fitted_churn_model.evaluate(test_churn)

# Calculate AUC
churn_eval = BinaryClassificationEvaluator(rawPredictionCol='prediction', labelCol='churn')
auc = churn_eval.evaluate(pred_and_labels.predictions)
print(auc)


# Predict on brand new unlabeled data
final_lr_model = lr_churn.fit(final_data)

new_customers = spark.read.csv('new_customers.csv',inferSchema=True,header=True)
test_new_customers = assembler.transform(new_customers) # create densevector
final_results = final_lr_model.transform(test_new_customers)
print(final_results.select('Company','prediction').show())
