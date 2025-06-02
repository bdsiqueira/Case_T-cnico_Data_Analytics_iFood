# Databricks notebook source
# MAGIC
# MAGIC %md
# MAGIC # 1.0 Importing Libraries

# COMMAND ----------

import tarfile
import pandas as pd
from io import BytesIO
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import datetime, timedelta, date
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import *
from dateutil.relativedelta import relativedelta
import json

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # 2.0 Parameter Settings

# COMMAND ----------

start_date = lit("2018-12-03").cast(TimestampType())

# COMMAND ----------

# MAGIC %md 
# MAGIC # 3.0 Reading Datasets

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3.1 order

# COMMAND ----------

!wget -O order.json.gz https://data-architect-test-source.s3-sa-east-1.amazonaws.com/order.json.gz
bronze_order = spark.read.json("file://"+"/databricks/driver/order.json.gz")

# COMMAND ----------

bronze_order.limit(10).display()

# COMMAND ----------

bronze_order\
    .select('customer_id','merchant_id','delivery_address_city','order_created_at','order_id','order_total_amount','delivery_address_country').describe().display()

# COMMAND ----------

# Verificar valores nulos
null_counts = bronze_order.select([count(when(col(c).isNull(), c)).alias(c) for c in bronze_order.columns])
null_counts.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.1.1 Creating Silver Dataset

# COMMAND ----------

#retirando nulos em customer_id e order_total_amount (8659 linhas) e selecionando campos relevantes para reduzir volume da tabela

silver_order = (bronze_order
            .filter(col('customer_id').isNotNull())
            .filter((col('order_total_amount')> 0.0)&(col('order_total_amount').isNotNull()))
            .drop('cpf','customer_name','origin_platform','delivery_address_country',                  'delivery_address_district','delivery_address_external_id','delivery_address_zip_code',
                  'order_scheduled','order_scheduled_date')
            )

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3.2 customer

# COMMAND ----------

!wget -O consumer.csv.gz https://data-architect-test-source.s3-sa-east-1.amazonaws.com/consumer.csv.gz
bronze_consumer = spark.read.option("header", "true") \
                       .option("inferSchema", "true") \
                       .csv("file:///databricks/driver/consumer.csv.gz")

# COMMAND ----------

bronze_consumer.limit(10).display()

# COMMAND ----------

# Verificar valores nulos
null_counts = bronze_consumer.select([count(when(col(c).isNull(), c)).alias(c) for c in bronze_consumer.columns])
null_counts.display()

# COMMAND ----------

# verificando volume por grupo e datas de criação de conta
bronze_consumer\
    .groupBy('active')\
        .agg(countDistinct('customer_id').alias('total_customer_id')
             ,min('created_at').alias('min_date')
             ,max('created_at').alias('max_date'))\
                 .display()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### 3.2.1 Creating Silver Dataset

# COMMAND ----------

#selecionando campos relevantes para reduzir volume da tabela

silver_consumer = (bronze_consumer
            .select('customer_id',
                    'created_at',
                    col('active').alias('customer_active')
                    )
            .distinct()
            )

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3.3 restaurant

# COMMAND ----------

!wget -O restaurant.csv.gz https://data-architect-test-source.s3-sa-east-1.amazonaws.com/restaurant.csv.gz
bronze_restaurant = spark.read.option("header", "true") \
                       .option("inferSchema", "true") \
                       .csv("file:///databricks/driver/restaurant.csv.gz")

# COMMAND ----------

bronze_restaurant.limit(10).display()

# COMMAND ----------

# Verificar valores nulos
null_counts = bronze_restaurant.select([count(when(col(c).isNull(), c)).alias(c) for c in bronze_restaurant.columns])
null_counts.display()

# COMMAND ----------

bronze_restaurant\
    .groupBy('enabled')\
        .agg(countDistinct('id').alias('total_customer_id')
             ,min('created_at').alias('min_date')
             ,max('created_at').alias('max_date'))\
                 .display()

# COMMAND ----------

# MAGIC
# MAGIC %md
# MAGIC
# MAGIC ### 3.3.1 Creating Silver Dataset

# COMMAND ----------

silver_restaurant = (bronze_restaurant
            .select(col('id').alias('merchant_id')
                    ,col('created_at').alias('merchant_created_at')
                    ,col('enabled').alias('merchant_active')
                    )
            .distinct()
            )

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3.4 ab_test_data

# COMMAND ----------

# Baixar o arquivo (se necessário)
!wget -O ab_test_ref.tar.gz https://data-architect-test-source.s3-sa-east-1.amazonaws.com/ab_test_ref.tar.gz

# Extrair e ler apenas o arquivo ab_test_ref.csv
with tarfile.open("ab_test_ref.tar.gz", "r:gz") as tar:
    # Extrair especificamente o arquivo ab_test_ref.csv (não o arquivo de metadados)
    file_data = tar.extractfile("ab_test_ref.csv")
    
    if file_data:
        # Ler como CSV e converter para Spark DataFrame
        pandas_df = pd.read_csv(BytesIO(file_data.read()))
        bronze_ab_test = spark.createDataFrame(pandas_df)

# COMMAND ----------

bronze_ab_test.limit(10).display()

# COMMAND ----------

bronze_ab_test.groupBy('is_target').count().display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.4.1 Creating Silver Dataset

# COMMAND ----------

silver_ab_test = bronze_ab_test.distinct()

# COMMAND ----------

silver_ab_test.count()

# COMMAND ----------

# MAGIC %md
# MAGIC # 4.0 Creating Gold Dataset

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4.1 join silver datasets

# COMMAND ----------

df_join = (silver_order
            .join(silver_consumer,['customer_id'],'left')
            .join(silver_restaurant,['merchant_id'],'left')
            .join(silver_ab_test,['customer_id'],'left')
            )

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4.2 df_discounts_orders

# COMMAND ----------

# Função para extrair todos os valores de totalDiscount
def extract_all_discounts(json_str):
    if not json_str:
        return []
        
    try:
        items = json.loads(json_str)
        discounts = []
        
        # Função para coletar recursivamente todos os descontos
        def collect_discounts(item):
            if isinstance(item, dict):
                if "totalDiscount" in item and "value" in item["totalDiscount"]:
                    discounts.append(item["totalDiscount"]["value"])
                
                # Processar recursivamente os garnishItems
                if "garnishItems" in item and isinstance(item["garnishItems"], list):
                    for garnish in item["garnishItems"]:
                        collect_discounts(garnish)
        
        # Processar cada item
        for item in items:
            collect_discounts(item)
            
        return discounts
    except Exception as e:
        print(f"Erro ao processar JSON: {e}")
        return []

# Registrar a UDF
extract_discounts_udf = udf(extract_all_discounts, ArrayType(StringType()))

# Adicionar a nova coluna ao DataFrame existente
df_discounts_orders =(df_join
                      .withColumn("discount_values",
                                   extract_discounts_udf(col("items")))
                      .withColumn("total_discount_sum",
                                   expr("aggregate(transform(discount_values, x -> cast(x as double)), 0D, (acc, x) -> acc + x)"))
                      )

# COMMAND ----------

# identificando se existem informações de valores de descontos aplicados pelo cupom
df_discounts_orders.limit(10).filter(col('total_discount_sum')>0).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4.3 df_agg_customer

# COMMAND ----------

# obervando volumes verais agregados por mes e grupos
df_join\
    .groupBy(date_trunc('mon', col('order_created_at')).alias('reference_month'),'is_target')\
        .agg(countDistinct(col('customer_id')).alias('total_customer'),
             countDistinct(col('order_id')).alias('total_order'),
             round(sum(col('order_total_amount')),2).alias('order_total'),
             countDistinct(col('order_id'))/countDistinct(col('customer_id'))).alias('total_customer')\
               .display()

# COMMAND ----------

df_agg_customer = (df_join
    .groupBy(date_trunc('month', col('order_created_at')).alias('reference_month'),
                          'customer_id',
                          'is_target')
                 .agg(countDistinct(col('order_id')).alias('total_order'),
                      round(sum(col('order_total_amount'))/countDistinct(col('order_id')),2).alias('ticket_medio'),
                      round(sum(col('order_total_amount')),2).alias('order_total_amount'),
                      countDistinct(col('merchant_id')).alias('total_merchant_id'))
        )


# COMMAND ----------

df_agg_customer.limit(10).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4.3 df_life_cycle

# COMMAND ----------

# Criar DataFrame com as duas datas
dates = [("2019-01-01",), ("2018-12-01",)]
schema = StructType([StructField("reference_month", StringType(), False)])
dates_df = spark.createDataFrame(dates, schema)

# COMMAND ----------

# MAGIC %md
# MAGIC  O lifecycle mensal categoriza os usuários de acordo com seu comportamento de compra entre períodos consecutivos, permitindo acompanhar a retenção, perda e recuperação de clientes.
# MAGIC
# MAGIC - Ativos: Usuários com pedidos em dezembro e janeiro
# MAGIC - Churn: Usuários sem pedidos em dezembro ou com pedidos em dezembro mas sem em janeiro
# MAGIC - Reativados: Usuários sem pedidos em dezembro que voltaram em janeiro
# MAGIC - Inativos: Usuários que são churn pelo segundo mês consecutivo

# COMMAND ----------

df_life_cycle = (silver_consumer
                 .select('customer_id','created_at')
                 .crossJoin(dates_df)
                 .join(silver_ab_test,['customer_id'],'inner')
                 .join(df_agg_customer.drop('is_target'),['customer_id','reference_month'],'left')
                 .withColumn("customer_aging", 
                round(abs(months_between(col("created_at").cast(TimestampType()), 
                                         col('reference_month'))), 0))
                 .withColumn('status_preliminar',
                             when(col('total_order')>0,lit('ativo'))
                             .otherwise(lit('churn'))
                             )
                 .withColumn('last_status',
                              lag(col('status_preliminar'),1).over(Window().partitionBy(col('customer_id')).orderBy(col('reference_month'))))
                .withColumn('status',
                             when(col('reference_month').isin('2018-12-01'),col('status_preliminar'))
                             .when(col('last_status').isin('ativo') 
                                   & col('status_preliminar').isin('ativo')
                                   ,lit('ativo'))
                             .when(col('last_status').isin('ativo') 
                                   & col('status_preliminar').isin('churn')
                                   ,lit('churn'))
                             .when(col('last_status').isin('churn') 
                                   & col('status_preliminar').isin('ativo')
                                   ,lit('reativado'))
                             .when(col('last_status').isin('churn') 
                                   & col('status_preliminar').isin('churn')
                                   ,lit('inativo'))
                             .otherwise(lit('nao_identificado'))
                             )
                .withColumn('total_order_lm',
                              lag(col('total_order'),1).over(Window().partitionBy(col('customer_id')).orderBy(col('reference_month'))))
                .withColumn('total_amount_lm',
                              lag(col('order_total_amount'),1).over(Window().partitionBy(col('customer_id')).orderBy(col('reference_month'))))
                .withColumn('ticket_medio_lm',
                              lag(col('ticket_medio'),1).over(Window().partitionBy(col('customer_id')).orderBy(col('reference_month'))))
                .withColumn('alteracao_frequencia',
                             when(col('reference_month').isin('2018-12-01'),lit('na'))
                             .when(col('total_order') > col('total_order_lm')
                                   ,lit('crescimento'))
                             .when(col('total_order') < col('total_order_lm')
                                   ,lit('retracao'))
                             .otherwise(lit('manutencao')))

                .select(col('reference_month').cast(TimestampType()),
                        'customer_id',
                        'is_target',
                        'customer_aging',
                        'status',
                        'last_status',
                        'total_order',
                        'total_order_lm', #last_month
                        'alteracao_frequencia',
                        col('order_total_amount').alias('total_amount'),
                        'total_amount_lm', #last_month
                        'ticket_medio',
                        'ticket_medio_lm' #last_month
                        )
                 )

# COMMAND ----------

df_life_cycle.limit(10).display()

# COMMAND ----------

# MAGIC %md
# MAGIC # 5.0 Analysis & Insights

# COMMAND ----------

# distribuição de pedidos por grupo controle x teste

pandas_df = df_agg_customer.toPandas()
pandas_df['month_label'] = pandas_df['reference_month'].dt.strftime('%Y-%m-%d')

plt.rcParams.update({
    'font.size': 14,
    'axes.labelsize': 16,
    'axes.titlesize': 20,
    'xtick.labelsize': 14,
    'ytick.labelsize': 14,
    'legend.fontsize': 14,
    'legend.title_fontsize': 16
})

plt.figure(figsize=(14, 8))
sns.boxplot(
    x='month_label',
    y='total_order', 
    hue='is_target',  
    data=pandas_df, 
    palette={'control': 'gray', 'target': 'red'},
    showfliers=False
)

plt.title('Distribuição de Total de Pedidos por Mês: Controle vs Teste')
plt.xlabel('Mês de Referência')
plt.ylabel('Total de Pedidos')
plt.xticks(rotation=0)
plt.tight_layout()
plt.savefig('boxplot_pedidos.png')


# COMMAND ----------

# distribuição de ticket_medio por grupo controle x teste
pandas_df = df_agg_customer.toPandas()
pandas_df['month_label'] = pandas_df['reference_month'].dt.strftime('%Y-%m-%d')

plt.rcParams.update({
    'font.size': 14,
    'axes.labelsize': 16,
    'axes.titlesize': 20,
    'xtick.labelsize': 14,
    'ytick.labelsize': 14,
    'legend.fontsize': 14,
    'legend.title_fontsize': 16
})

plt.figure(figsize=(14, 8))
sns.boxplot(
    x='month_label',
    y='ticket_medio', 
    hue='is_target',  
    data=pandas_df,
    palette={'control': 'gray', 'target': 'red'},
    showfliers=False
)

plt.title('Distribuição de Ticket Medio por Mês: Controle vs Teste')
plt.xlabel('Mês de Referência')
plt.ylabel('Ticket Medio')
plt.xticks(rotation=0)
plt.tight_layout()
plt.savefig('boxplot_ticket.png')


# COMMAND ----------

# distribuição de pedidos por grupo controle x teste filtro de status
pandas_df = df_life_cycle.filter(col('status').isin('reativado')).toPandas()
pandas_df['month_label'] = pandas_df['reference_month'].dt.strftime('%Y-%m-%d')

plt.rcParams.update({
    'font.size': 14,
    'axes.labelsize': 16,
    'axes.titlesize': 20,
    'xtick.labelsize': 14,
    'ytick.labelsize': 14,
    'legend.fontsize': 14,
    'legend.title_fontsize': 16
})

plt.figure(figsize=(14, 8))
sns.boxplot(
    x='month_label',
    y='total_order', 
    hue='is_target',  
    data=pandas_df, 
    palette={'control': 'gray', 'target': 'red'},
    showfliers=False
)

plt.title('Distribuição de Total de Pedidos por Mês: Controle vs Teste')
plt.xlabel('Mês de Referência')
plt.ylabel('Total de Pedidos')
plt.xticks(rotation=0)
plt.tight_layout()
plt.savefig('boxplot_pedidos.png')


# COMMAND ----------

df_life_cycle\
    .groupBy('reference_month','is_target')\
        .pivot('status')\
        .agg(countDistinct('customer_id').alias('total_customer'))\
            .display()

# COMMAND ----------

df_life_cycle\
    .groupBy('reference_month','is_target','customer_aging')\
        .pivot('status')\
        .agg(countDistinct('customer_id').alias('total_customer'))\
            .display()

# COMMAND ----------

df_life_cycle\
    .groupBy('reference_month','is_target')\
        .pivot('alteracao_frequencia')\
        .agg(countDistinct('customer_id').alias('total_customer'))\
            .display()

# COMMAND ----------

df_life_cycle\
    .groupBy('reference_month','is_target','status')\
        .pivot('alteracao_frequencia')\
        .agg(countDistinct('customer_id').alias('total_customer'))\
            .display()

# COMMAND ----------

df_life_cycle\
    .groupBy('is_target')\
        .pivot('reference_month')\
            .agg(sum('total_amount').alias('total_amount'))\
            .display()

# COMMAND ----------

df_life_cycle\
    .groupBy('is_target')\
        .pivot('reference_month')\
            .agg(sum('total_order').alias('total_order'))\
            .display()

# COMMAND ----------

df_life_cycle\
    .groupBy('is_target','status')\
        .pivot('reference_month')\
            .agg(sum('total_amount').alias('total_amount'))\
            .display()
        