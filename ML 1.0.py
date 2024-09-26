# Databricks notebook source
#!pip install mlflow

# COMMAND ----------

#!pip install imblearn

# COMMAND ----------

from imblearn.over_sampling import SMOTE

# COMMAND ----------

from pyspark.sql.functions import col

# Load the silver Delta table as a Spark DataFrame
silver_data_df = spark.read.format("delta").table("hive_metastore.default.silver_data_new")

# COMMAND ----------

silver_data_df.show()

# COMMAND ----------

# Drop rows with null values
cleaned_silver_data_df = silver_data_df.na.drop()

# Show the cleaned DataFrame
cleaned_silver_data_df.show()

# COMMAND ----------

print(cleaned_silver_data_df.count())
print(len(cleaned_silver_data_df.columns))

# COMMAND ----------

training_data = cleaned_silver_data_df.select(
    col("SignalStrength"),
    col("CallDropRate"),
    col("DataTransferSpeed"),
    col("Region"),
    col("CustomerSatisfaction"),
    col("ComplaintType") # Target column
)

training_data.show()

# COMMAND ----------

from pyspark.ml.feature import StringIndexer, VectorAssembler
from pyspark.ml import Pipeline

# Index categorical columns (Region, ComplaintType)
region_indexer = StringIndexer(inputCol="Region", outputCol="RegionIndex")
complaint_type_indexer = StringIndexer(inputCol="ComplaintType", outputCol="label") # Target

# COMMAND ----------

training_data.show(5)

# COMMAND ----------

feature_assembler = VectorAssembler(
    inputCols=["SignalStrength", "CallDropRate", "DataTransferSpeed", "RegionIndex", "CustomerSatisfaction"],
    outputCol="features"
)

# COMMAND ----------

preprocessing_pipeline = Pipeline(stages=[region_indexer, complaint_type_indexer, feature_assembler])

# COMMAND ----------

preprocessed_data = preprocessing_pipeline.fit(training_data).transform(training_data)

# COMMAND ----------

preprocessed_data.show(5)

# COMMAND ----------

import pandas as pd
from pyspark.ml.feature import VectorAssembler
from imblearn.over_sampling import SMOTE

# Assemble the features into a single vector column
assembler = VectorAssembler(
    inputCols=[
        "SignalStrength", 
        "CallDropRate", 
        "DataTransferSpeed", 
        "RegionIndex", 
        "CustomerSatisfaction"
    ],
    outputCol="features"
)
preprocessed_data = assembler.transform(preprocessed_data)

# Convert the preprocessed data into a Pandas DataFrame
pandas_data = preprocessed_data.select("label", "features").toPandas()

# Separate features and labels for SMOTE
X = pd.DataFrame(pandas_data['features'].tolist())  # Convert the 'features' vector column into separate feature columns
y = pandas_data['label']

# Apply SMOTE to balance the classes
smote = SMOTE(sampling_strategy='auto', random_state=123)
X_resampled, y_resampled = smote.fit_resample(X, y)

# Combine the resampled features and labels back into a DataFrame
resampled_df = pd.concat([X_resampled, pd.Series(y_resampled, name='label')], axis=1)

# Convert back to Spark DataFrame
oversampled_spark_df = spark.createDataFrame(resampled_df)

# Assemble the features back into a single vector column for the model training
assembler = VectorAssembler(
    inputCols=oversampled_spark_df.columns[:-1],  # All columns except the label column
    outputCol="features"
)

# Transform the oversampled DataFrame
oversampled_spark_df = assembler.transform(oversampled_spark_df)

display(oversampled_spark_df)

# COMMAND ----------

#!pip install mlflow

# COMMAND ----------

import mlflow
mlflow.set_experiment(experiment_id="4286520757199339")

mlflow.autolog()

# COMMAND ----------

train_data, test_data = oversampled_spark_df.randomSplit([0.8, 0.2], seed=123)
train_data = train_data.limit(500)
#test_data = test_data.limit(200)

# COMMAND ----------

train_data.count()

# COMMAND ----------

preprocessed_data.show()

# COMMAND ----------

from pyspark.ml.feature import StringIndexer
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml import Pipeline

# Define the StringIndexer with handleInvalid set to 'keep'
indexer = StringIndexer(inputCol="label", outputCol="indexedLabel", handleInvalid="keep")

# Define the RandomForestClassifier
rf_classifier = RandomForestClassifier(labelCol="indexedLabel", featuresCol="features")

# Create a pipeline with the indexer and classifier
pipeline = Pipeline(stages=[indexer, rf_classifier])

# Train the model
from pyspark.ml.tuning import ParamGridBuilder, CrossValidator
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
# Define the hyperparameter grid
paramGrid = (ParamGridBuilder()
             .addGrid(rf_classifier.numTrees, [50, 100, 150])  # Tuning number of trees
             .addGrid(rf_classifier.maxDepth, [5, 10, 15])     # Tuning tree depth
             .addGrid(rf_classifier.maxBins, [32, 64])         # Tuning maxBins
             .build())

evaluator = MulticlassClassificationEvaluator(
    labelCol="label",  # Column containing the true labels
    predictionCol="prediction",  # Column containing the predicted labels
    metricName="accuracy"  # Metric to evaluate (accuracy, f1, weightedPrecision, etc.)
)
# Set up 3-fold CrossValidator
crossval = CrossValidator(estimator=pipeline,
                          estimatorParamMaps=paramGrid,
                          evaluator=evaluator,
                          numFolds=3)

# Train the model using cross-validation
cv_model = crossval.fit(train_data)

# Evaluate accuracy on the test set
cv_predictions = cv_model.transform(test_data)
cv_accuracy = evaluator.evaluate(cv_predictions)
print(f"Cross-validated Test Accuracy = {cv_accuracy:.2f}")


# COMMAND ----------

import pandas as pd
from sklearn.metrics import confusion_matrix
import matplotlib.pyplot as plt
import seaborn as sns

# Convert predictions and true labels to Pandas
predictions_pandas = cv_predictions.select("label", "prediction").toPandas()
#print(predictions_pandas)
# Extract true labels and predictions
y_true = predictions_pandas["label"]
y_pred = predictions_pandas["prediction"]

# COMMAND ----------

# Compute the confusion matrix
conf_matrix = confusion_matrix(y_true, y_pred)
# Set up the matplotlib figure
plt.figure(figsize=(8, 6))

# Use Seaborn to create a heatmap for the confusion matrix
sns.heatmap(conf_matrix, annot=True, fmt='g', cmap='Blues')

# Add titles and labels
plt.title('Confusion Matrix')
plt.xlabel('Predicted Labels')
plt.ylabel('True Labels')

# Show the plot
plt.show()


# COMMAND ----------

# Extract feature importances from the trained RandomForest model
rf_model = cv_model.bestModel.stages[-1]  # Last stage is the RandomForestClassifier
feature_importances = rf_model.featureImportances.toArray()

# Define feature names
feature_names = ["SignalStrength", "CallDropRate", "DataTransferSpeed", "Region", "CustomerSatisfaction"]

# Create a pandas DataFrame to store feature importances
import pandas as pd
import seaborn as sns

# Create a DataFrame for feature importances
feature_importance_df = pd.DataFrame({
    'Feature': feature_names,
    'Importance': feature_importances
})

# Sort by importance
feature_importance_df = feature_importance_df.sort_values(by='Importance', ascending=False)

# Plot feature importances
plt.figure(figsize=(8, 6))
sns.barplot(x='Importance', y='Feature', data=feature_importance_df)
plt.title('Feature Importance Plot')
plt.xlabel('Importance')
plt.ylabel('Feature')
plt.show()

# COMMAND ----------

