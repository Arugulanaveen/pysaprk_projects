# Databricks notebook source
class DataSink:
    '''
    Abstract class
    '''
    def __init__(self,df,path,method,params):
        self.df=df
        self.path=path
        self.method=method
        self.params=params

    def load_data_frame(self):
        pass


class LoadToDBFS(DataSink):

    def load_data_frame(self):
        self.df.write.mode(self.method).save(self.path)

class LoadToDBFSWithPartition(DataSink):

    def load_data_frame(self):
        partitionByColumns=self.params.get("partitionByColumns")
        self.df.write.mode(self.method).partitionBy(*partitionByColumns).save(self.path)


class LoadToDeltaTable(DataSink):

    def load_data_frame(self):
        
        self.df.write.format('delta').mode(self.method).saveAsTable(self.path)


def get_sink_source(df,sink_type,path,method,params=None):

    if sink_type=='dbfs':
        return LoadToDBFS(df,path,method,params)
    elif sink_type=='dbfs_with_partition':
        return LoadToDBFSWithPartition(df,path,method,params)
    elif sink_type=='delta':
        return LoadToDeltaTable(df,path,method,params)
    else:
        return ValueError(f"Not implemented for sink_type : {sink_type}")


# COMMAND ----------

