# Databricks notebook source
# MAGIC %run "./reader"
# MAGIC

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import lead,broadcast,collect_set,col,array_contains,datediff,split,sum,desc


class Transformer:
    def __init__(self):
        pass
    def transform(self,inputDFs):
        pass

class FirstTransformer(Transformer):

    def transform(self,inputDFs):

        '''
        Customers who have bought Airpods after buying the  iphone
        
        '''
        transactionInputDF=inputDFs.get("transactionInputDF")

        windowSpec=Window.partitionBy("customer_id").orderBy("transaction_date")
        transactionDF=transactionInputDF.withColumn(
            "next_product_bought",lead("product_name").over(windowSpec)
        )

        # "Airpods right after buying Iphone" 

        filteredDF=transactionDF.where("product_name='iPhone' and next_product_bought='AirPods'")

        customerinputDF=inputDFs.get("customerinputDF")
        #partition  on the join column before broadcast so that the  same data  resides on a single executor
        filteredDF=filteredDF.repartition("customer_id")

        joinDF=filteredDF.join(broadcast(customerinputDF),"customer_id","inner")
        joinDF=joinDF.select(
            "customer_id",
            "customer_name",
            "location"
        )

        return joinDF

class SecondTransformer(Transformer):

    def transform(self,inputDFs):

        ''' customer who bought iphone and airpods '''

        filteredDF=inputDFs.get("transactionInputDF").where("product_name='iPhone' or product_name='AirPods' ")
       
        #customers 
        set_of_products_per_customerDF=filteredDF.groupBy("customer_id").agg(collect_set("product_name").alias("set_of_products"))
        iphone_and_airpodsDF=set_of_products_per_customerDF.filter(array_contains(col("set_of_products"),"iPhone") & array_contains(col("set_of_products"),"AirPods"))
        customerinputDF=inputDFs.get("customerinputDF")
        joinDF=set_of_products_per_customerDF.join(broadcast(customerinputDF),"customer_id","inner")
        joinDF=joinDF.select(
            "customer_id",
            "customer_name",
            "location"
        )
        return joinDF
    
class AllProductsAfterInitialPurchaseTransformer(Transformer):

    def transform(self,inputDFs):

        ''' all accesorries after their first '''

        transactionInputDF=inputDFs.get("transactionInputDF")
        customerinputDF=inputDFs.get("customerinputDF")
        transactionInputDF.createOrReplaceTempView("transactionInputDF")
        customerinputDF.createOrReplaceTempView("customerinputDF")


        
        allproductsafterinitialDF=spark.sql('''
                  with rankedtransactionbypurchasedateforcustomerDF (
                      select 
                      customer_id
                      ,product_name
                      ,row_number() over(partition by customer_id order by transaction_date desc) as rn
                      from transactionInputDF 
                  ) ,
                    fileredProductsDF
                   (select 
                   customer_id
                   ,product_name
                    from 
                    rankedtransactionbypurchasedateforcustomerDF
                    where rn!=1
                    )
                  ,
                  productsafterinitialDF (
                      select
                      collect_list(product_name) as productsafterfirst
                      ,customer_id
                      from fileredProductsDF
                      group  by 2

                  )
                  select * from productsafterinitialDF

                  ''')
    
        return allproductsafterinitialDF



class TimeDelayInbuyingAirpodsafterIphone(Transformer):



    def transform(self,inputDFs):


        transactionInputDF=inputDFs.get("transactionInputDF")
        customerinputDF=inputDFs.get("customerinputDF")

        windowspec=Window.partitionBy("customer_id").orderBy("transaction_date")
        windowtransactionDF=transactionInputDF.withColumn("next_product",lead('product_name').over(windowspec))\
        .withColumn("second_product_transaction_date",lead('transaction_date').over(windowspec))

        airpodsafteriphonetransactionDF=windowtransactionDF.where("product_name='iPhone' and next_product = 'AirPods' ")
        
        days_delayed_for_buying_airpods_after_phone_DF=airpodsafteriphonetransactionDF.withColumn("no_of_days_delayed",datediff("second_product_transaction_date","transaction_date"))

        #partiotin  on the join column before broadcast so that the  same data  resides on a single executor
        days_delayed_for_buying_airpods_after_phone_DF=days_delayed_for_buying_airpods_after_phone_DF.repartition("customer_id")

        joinDF=days_delayed_for_buying_airpods_after_phone_DF.join(broadcast(customerinputDF),"customer_id","inner")
        joinDF=joinDF.select(
            "customer_id",
            "customer_name",
            "no_of_days_delayed",
            "location"
        )

        return joinDF
    

    
class TopThreeSellingProductsbyRevenueTransformer(Transformer):

    def transform(self,inputDFs):
        transactionInputDF=inputDFs.get("transactionInputDF")
        customerinputDF=inputDFs.get("customerinputDF")
        productsinputDF=inputDFs.get("productsinputDF")

        filteredDF=productsinputDF.withColumn("first_word", split("product_name", " ")[0])
        transactionsandproductsjoinedDF=transactionInputDF.join(broadcast(filteredDF),transactionInputDF.product_name==filteredDF.first_word,'left')
        top3categorydf=transactionsandproductsjoinedDF.groupBy("category").agg(sum("price").alias("total_sales_price")).orderBy(desc("total_sales_price")).limit(3)
        return top3categorydf


# COMMAND ----------

