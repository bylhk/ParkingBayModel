from abc import abstractmethod
import os
from pyspark.sql import SparkSession, functions as sf
from pyspark.sql.types import ArrayType, DoubleType

from config.data import (SPARK_NAME,
                         DATA_DIR,
                         RAW_DIR,
                         BG_NAME,
                         BG_TABLE,
                         BG_SCHEMA,
                         BR_NAME,
                         BR_TABLE,
                         BR_SCHEMA,
                         SR_NAME,
                         SR_TABLE,
                         SR_SCHEMA,
                         )
from lib.graph import str_to_latlong

geo_udf = sf.udf(lambda x: str_to_latlong(x), ArrayType(DoubleType()))


class SparkData:
    def __init__(
        self, 
        data_dir=DATA_DIR,
    ):
        self.data_dir = data_dir
        self.spark = SparkSession.builder.appName(SPARK_NAME).getOrCreate()
        self._load()
    
    @abstractmethod
    def _load(self):
        """Load data to spark"""
        
    def query(self, query, df=True):
        if df:
            return self.spark.sql(query).toPandas()
        else:
            return self.spark.sql(query)
    

class ParkingData(SparkData):
    def __init__(
        self,
        data_dir=DATA_DIR,
    ):
        super().__init__(
            data_dir=data_dir,
        )

    def _load(self):
        self.bg_data = self.spark.read.csv(
            os.path.join(self.data_dir, RAW_DIR, BG_NAME),
            schema=BG_SCHEMA,
            header=True,
            timestampFormat='yyyyMMddHHmmss',
        ).withColumn('latlong', geo_udf(sf.col('the_geom')))
        self.bg_data.createOrReplaceTempView(BG_TABLE)

        self.br_data = self.spark.read.csv(
            os.path.join(self.data_dir, RAW_DIR, BR_NAME), 
            schema=BR_SCHEMA,
            header=True,
            timestampFormat='HH:mm:ss',
        )
        self.br_data.createOrReplaceTempView(BR_TABLE)

        self.sr_data = self.spark.read.csv(
            os.path.join(self.data_dir, RAW_DIR, SR_NAME),
            schema=SR_SCHEMA,
            header=True,
            timestampFormat='MM/dd/yyyy hh:mm:ss a',
        )
        self.sr_data.createOrReplaceTempView(SR_TABLE)
    