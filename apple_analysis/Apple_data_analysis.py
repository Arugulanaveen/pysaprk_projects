# Databricks notebook source
# MAGIC %run "./reader"

# COMMAND ----------

# MAGIC %run "./transformer"

# COMMAND ----------

# MAGIC %run "./extractor"

# COMMAND ----------

# MAGIC %run "./loader"

# COMMAND ----------

# MAGIC %run "./loader_factory"

# COMMAND ----------

class FirstWorkFlow:
    ''' ETL  pipeline for extracting and loading the customer details who bought airpods after buying iphone'''
    def __init__(self):
        pass

    def runner(self):

        #Step 1 : extract all required data from different source for analysis
        inputDFs=AirPadsafteriPhoneextractor().extract()

        #Step 2: implement the transfomation logic 
        # customers who bought airpods after buying the iphone
        firstTransformedDF=FirstTransformer().transform(inputDFs)

        #step 3 : load the tranformed data into the sink
        AirPodsAfterIphoneLoader(firstTransformedDF).sink()
        return firstTransformedDF


# COMMAND ----------

class SecondWorkFlow:
    ''' ETL  pipeline for extracting and loading the customer details who bought airpods and  iphone'''
    def __init__(self):
        pass

    def runner(self):

        #Step 1 : extract all required data from different source for analysis
        inputDFs=AirPadsafteriPhoneextractor().extract()

        #Step 2: implement the transfomation logic 
        # customers who bought airpods and bought the iphone
        SecondTransformedDF=SecondTransformer().transform(inputDFs)

        #step 3 : load the tranformed data into the sink
        AirPodsandIphoneLoader(SecondTransformedDF).sink()
        return SecondTransformedDF


# COMMAND ----------

class AllProductsAfterInitialPurchase:

    def __init__(self):
        pass

    def runner(self):

        #Step 1 : extract all required data from different source for analysis
        inputDFs=AirPadsafteriPhoneextractor().extract()

        #Step 2: implement the transfomation logic 
        
        AllProductsAfterInitialPurchaseDF=AllProductsAfterInitialPurchaseTransformer().transform(inputDFs)

        #step 3 : load the tranformed data into the sink
        AllProductsAfterInitialPurchaseLoader(AllProductsAfterInitialPurchaseDF).sink()
        return AllProductsAfterInitialPurchaseDF

    
    

# COMMAND ----------

class TimeDelayInBuyingAirpodsAfterIphone:

    def __init__(self):
        pass

    def runner(self):

        #Step 1 : extract all required data from different source for analysis
        inputDFs=AirPadsafteriPhoneextractor().extract()

        #Step 2: implement the transfomation logic 
        # time delay in buying  airpods after iphone
        TimeDelayInbuyingAirpodsafterIphoneDF=TimeDelayInbuyingAirpodsafterIphone().transform(inputDFs)

        #step 3 : load the tranformed data into the sink
        TimeDelayInBuyingAirpodsAfterIphoneLoader(TimeDelayInbuyingAirpodsafterIphoneDF).sink()
        return TimeDelayInbuyingAirpodsafterIphoneDF


# COMMAND ----------

class TopThreeSellingProductsbyRevenue:

    def __init__(self):
        pass

    def runner(self):

        #Step 1 : extract all required data from different source for analysis
        inputDFs=AirPadsafteriPhoneextractor().extract()

        #Step 2: implement the transfomation logic 
        # time delay in buying  airpods after iphone
        TopThreeSellingProductsbyRevenueDF=TopThreeSellingProductsbyRevenueTransformer().transform(inputDFs)

        #step 3 : load the tranformed data into the sink
        TopThreeSellingProductsbyRevenueLoader(TopThreeSellingProductsbyRevenueDF).sink()
        
        return TopThreeSellingProductsbyRevenueDF


# COMMAND ----------

class WorkFlowRunner:

    def __init__(self,name):
        self.name=name

    def runner(self):
        if self.name=='firstworkflow':
            return FirstWorkFlow().runner()
        elif self.name=="secondworkflow":
            return SecondWorkFlow().runner()
        elif self.name=="allproductsafterinitialpurchase":
            return AllProductsAfterInitialPurchase().runner()
        elif self.name=="timedalyinbuyingairpodsafteriphone":
            return TimeDelayInBuyingAirpodsAfterIphone().runner()
        elif self.name=="top3sellingproductsbycategory":
            return TopThreeSellingProductsbyRevenue().runner()        


        
name='top3sellingproductsbycategory'
df=WorkFlowRunner(name).runner()
df.show()

# COMMAND ----------

from pyspark.sql import SparkSession
spark=SparkSession.builder.appName("thebigdataproject").getOrCreate()

# COMMAND ----------

input_df=spark.read.format("csv").option("header","true").option("inferschema","true").load("dbfs:/FileStore/Transaction_Updated.csv")

# COMMAND ----------

input_df.show()

# COMMAND ----------

input_df=spark.read.format("csv").option("header","true").option("inferschema","true").load("dbfs:/FileStore/Customer_Updated.csv")
input_df.show()

# COMMAND ----------

products=spark.read.format("csv").option("inferschema",'true').option("header","true").load("dbfs:/FileStore/Products_Updated.csv")
products.show()