# TRAITEMENT SPARK ROBUSTE AVEC GESTION DES NULLS
import findspark
findspark.init()
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, count, when, isnull, isnan, desc, to_date,current_date,mean ,lit,substring
from pyspark.sql.types import StructType, StringType, IntegerType, DoubleType, TimestampType
import time
import os
project_root = os.getcwd()               
output_dir   = os.path.join(project_root, "cleaned_cdrs")
def is_valid_number(col_name: str):
    pattern = r"""^(
        212[67]\d{8}|
        1\d{10}|
        33\d{9}|
        44\d{10}|
        49\d{11}|
        34\d{9}|
        39\d{10}|
        7\d{10}|
        81\d{10}|
        971\d{9}|
        966\d{9}
    )$"""
    # on enlève les espaces pour Spark
    pattern = pattern.replace(" ", "").replace("\n", "")
    return col(col_name).rlike(pattern) 

def traiter_voice(df):
    return df.dropna(subset=["caller_id"]) \
             .filter(col("duration_sec") > 0).filter( is_valid_number("caller_id"))\
             .fillna({
                 'cell_id': 'inconnu',
                 'technology': '4G', # Valeur par défaut
                 'callee_id': 'inconnu'
             }) \
              .withColumn("technology",
                 when(col("technology") == "LTE", "4G")
                 .when(col("technology") == "NR", "5G")
                 .when(col("technology") == "UMTS", "3G")
                 .otherwise(col("technology"))  
             ) \
              .withColumn("callee_id",
                when(~is_valid_number("callee_id"),"inconnu")
                .otherwise(col("callee_id"))
             )\
             .withColumn("rating_status",lit("ready")) \
             .withColumn("callee_cc", substring(col("callee_id"), 1, 3)) \
             .withColumn("product_code",
                when(col("callee_cc")=="inc","VOICE_NAT")
                .when(col("callee_cc") != "212", "VOICE_INT")
                .otherwise("VOICE_NAT")
             ) \
             .drop("callee_cc")

def traiter_sms(df):
    return df.dropna(subset=["sender_id"]) \
             .fillna({
                 'cell_id': 'inconnu',
                 'technology': '4G',
                 'receiver_id' :'inconnu'
             }) \
             .filter( is_valid_number("sender_id")) \
             .withColumn("technology",
                 when(col("technology") == "LTE", "4G")
                 .when(col("technology") == "NR", "5G")
                 .when(col("technology") == "UMTS", "3G")
                 .otherwise(col("technology"))  
             ) \
             .withColumn("receiver_id",
                when(~is_valid_number("receiver_id"),"inconnu")
                .otherwise(col("receiver_id"))
            ) \
             .withColumn("rating_status",lit("ready")) \
             .withColumn("product_code",lit("SMS_STD"))

def traiter_data(df):
    fallback = "inconnu"
    return df.dropna(subset=["user_id"]) \
             .filter( is_valid_number("user_id"))\
             .fillna({
                 'cell_id': 'inconnu',
                 'technology': '4G'
             }) \
             .withColumn("technology",
                 when(col("technology") == "LTE", "4G")
                 .when(col("technology") == "NR", "5G")
                 .when(col("technology") == "UMTS", "3G")
                 .otherwise(col("technology")) 
             ) \
             .withColumn("data_volume_mb_imputed",
              when(col("data_volume_mb").isNull(), lit(fallback))
              .otherwise(col("data_volume_mb"))
             ) \
             .withColumn("rating_status",
               when(
                ( col("data_volume_mb").isNull() ) |
                ( col("session_duration_sec").isNull() ) |
                ( col("session_duration_sec") < 0 ),
                lit("needs_review")
                    )
              .otherwise(lit("ready"))
             ) \
             .withColumn("session_duration_sec",
              when( (col("session_duration_sec").isNull()) | ( col("session_duration_sec") < 0 ), lit(fallback))
              .otherwise(col("session_duration_sec"))
             ) \
             .drop("data_volume_mb") \
             .withColumnRenamed("data_volume_mb_imputed", "data_volume_mb")
# 1. Spark Session
spark = SparkSession.builder \
    .appName("KafkaCDRReader") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0")\
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# 2. Schéma des données JSON
cdr_schema = StructType() \
    .add("record_ID", StringType()) \
    .add("record_type", StringType()) \
    .add("timestamp", TimestampType()) \
    .add("cell_id", StringType()) \
    .add("technology", StringType()) \
    .add("caller_id", StringType()) \
    .add("callee_id", StringType()) \
    .add("duration_sec", IntegerType()) \
    .add("sender_id", StringType()) \
    .add("receiver_id", StringType()) \
    .add("user_id", StringType()) \
    .add("data_volume_mb", DoubleType()) \
    .add("session_duration_sec", IntegerType())\
    .add("product_code", StringType())   

# 3. Lire depuis Kafka
df_kafka = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092")\
    .option("subscribe", "topic") \
    .option("startingOffsets", "latest") \
    .option("failOnDataLoss", "false") \
    .load()

# 4. Parser les valeurs JSON
df_parsed = df_kafka.selectExpr("CAST(value AS STRING) as json_string") \
    .select(from_json(col("json_string"), cdr_schema).alias("data")) \
    .select("data.*")

# 5. Traitement robuste avec gestion des nulls
# On s'assure que record_type est présent car c'est notre clé de catégorisation
df_valid = df_parsed.filter(col("record_type").isNotNull()& col("timestamp").isNotNull())
df_valid = df_valid.filter(
    to_date(col("timestamp"), "yyyy-MM-dd") >= current_date())
# 6. Afficher sur la console avec statistiques sur les nulls
def traiter(df, epoch_id):
    # Compter les enregistrements
    print(f"\n--- Batch {epoch_id} ---")
    total_count = df.count()
    
    if total_count > 0:
        
    
    # Traitement spécifique par type d'enregistrement
        print("\nExemples d'enregistrements par type:")
        
        # Voice records
        voice_df = df.filter(col("record_type") == "voice") \
                .drop("sender_id", "receiver_id", "user_id", "data_volume_mb", "session_duration_sec")
        voice_df= traiter_voice(voice_df)
        
        voice_count = voice_df.count()
        if voice_count > 0:
            voice_df.show(truncate=False)
        # SMS records
        sms_df = df.filter(col("record_type") == "sms") \
                .drop("caller_id", "callee_id", "duration_sec", "user_id", "data_volume_mb", "session_duration_sec")
        sms_df= traiter_sms(sms_df)
        
        sms_count = sms_df.count()
        if sms_count > 0:
            sms_df.show(truncate=False)
        # Data records
        data_df = df.filter(col("record_type") == "data") \
                    .drop("sender_id", "receiver_id", "caller_id", "callee_id", "duration_sec")
        data_df= traiter_data(data_df)
        
        data_count = data_df.count()
        if data_count > 0:
            data_df.show(truncate=False)
        # 1. Concaténation des trois flux nettoyés
        cleaned_batch_df = voice_df \
            .unionByName(sms_df, allowMissingColumns=True) \
            .unionByName(data_df, allowMissingColumns=True) 

        # 2. Écriture du batch nettoyé dans un dossier Parquet
        (cleaned_batch_df
        .write
        .mode("append")
        .partitionBy("record_type")            # pour accélérer les lectures ultérieures
        .parquet("cleand_df"))

# 7. Démarrer le stream avec foreachBatch pour un traitement personnalisé
query = df_valid.writeStream \
    .foreachBatch(traiter) \
    .outputMode("append") \
    .start()

try:
    query.awaitTermination()
except KeyboardInterrupt:
    query.stop()