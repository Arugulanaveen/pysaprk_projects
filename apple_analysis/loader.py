# Databricks notebook source
# MAGIC %run "./loader_factory"

# COMMAND ----------

class AbstractLoader:

    def __init__(self,transformedDF):
        self.transformedDF=transformedDF

    def  sink(self):
        pass

class AirPodsAfterIphoneLoader(AbstractLoader):

    def sink(self):
        get_sink_source(
            sink_type='dbfs',
            df=self.transformedDF,
            path='dbfs:/FileStore/tables/apple_data_analysis/output/airpodsAfterIphone',
            method='overwrite'

        ).load_data_frame()

class AirPodsandIphoneLoader(AbstractLoader):

    def sink(self):
        params={"partitionByColumns":["location"]}
        get_sink_source(
            sink_type='dbfs_with_partition',
            df=self.transformedDF,
            path='dbfs:/FileStore/tables/apple_data_analysis/output/partition/airpodsAndIphone',
            method='overwrite',
            params=params

        ).load_data_frame()

        get_sink_source(
            sink_type='delta',
            df=self.transformedDF,
            path='default.airpodsAndIphone',
            method='overwrite',
            params=params

        ).load_data_frame()

class AllProductsAfterInitialPurchaseLoader(AbstractLoader):

    def sink(self):
        get_sink_source(
            sink_type='dbfs',
            df=self.transformedDF,
            path='dbfs:/FileStore/tables/apple_data_analysis/output/AllProductsAfterInitialPurchase',
            method='overwrite'

        ).load_data_frame()


class TimeDelayInBuyingAirpodsAfterIphoneLoader(AbstractLoader):

    def sink(self):
        get_sink_source(
            sink_type='dbfs',
            df=self.transformedDF,
            path='dbfs:/FileStore/tables/apple_data_analysis/output/TimeDelayInBuyingAirpodsAfterIphone',
            method='overwrite'

        ).load_data_frame()

class TopThreeSellingProductsbyRevenueLoader(AbstractLoader):

    def sink(self):
        get_sink_source(
            sink_type='dbfs',
            df=self.transformedDF,
            path='dbfs:/FileStore/tables/apple_data_analysis/output/TopThreeSellingProductsbyRevenue',
            method='overwrite'

        ).load_data_frame()


# COMMAND ----------

