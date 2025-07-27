# Databricks notebook source
class DataSource:
    '''
    Abstract class 
    
    '''
    def __init__(self,path):
        self.path=path

    def get_data_frame(self):
        '''
        Abstract method ,  this function will be modified in the subclasses
        '''
        raise ValueError("Not Implemented")



class CSVDataSource(DataSource):

    def get_data_frame(self):

        return (
            spark
            .read
            .format("csv").
            option("header","true")
            .load(self.path)
            )
        
class ParquetDataSource(DataSource):

    def get_data_frame(self):

        return (
            spark
            .read
            .format("parquet")
            .load(self.path)
            )        

class OrcDataSource(DataSource):

    def get_data_frame(self):

        return (
            spark
            .read
            .format("orc")
            .load(self.path)
            )            


class DeltaDataSource(DataSource):

    def get_data_frame(self):

        return (
            spark
            .read
            .format("delta")
            .table(self.path)
            )    

def get_data_source(data_type,file_path):

    if data_type=='csv':
        return CSVDataSource(file_path)
    elif data_type=='parquet':
        return ParquetDataSource(file_path)
    elif data_type=='delta':
        return DeltaDataSource(file_path)
    elif data_type=='parquet':
        return ParquetDataSource(file_path)
    elif data_type=='orc':
        return OrcDataSource(file_path)          
    else:
        raise ValueError(f"Not implemented for {data_type}") 
   


# COMMAND ----------

