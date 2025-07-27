# Databricks notebook source
# MAGIC %run "./reader"

# COMMAND ----------

class Extractor:
    '''
    Abstract class
    '''
    def __init__(self):
        pass

    def extract(self):
        pass

class AirPadsafteriPhoneextractor(Extractor):
    '''
    implement this steps to extract or read the required data
    '''

    def extract(self):

        transactionInputDF=get_data_source(
        data_type="csv",
        file_path="dbfs:/FileStore/Transaction_Updated.csv"
        ).get_data_frame()
        
        customerinputDF=get_data_source(
            data_type="csv",
            file_path="dbfs:/FileStore/Customer_Updated.csv"
        ).get_data_frame()


        productsinputDF=get_data_source(
            data_type="csv",
            file_path="dbfs:/FileStore/Products_Updated.csv"
        ).get_data_frame()

        inputDFs={ "transactionInputDF":transactionInputDF,
                  "customerinputDF":customerinputDF,
                  "productsinputDF":productsinputDF
        }
        return inputDFs
