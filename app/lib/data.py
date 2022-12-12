from abc import abstractmethod
import os
import numpy as np
from pyspark.sql import SparkSession, functions as sf
from pyspark.sql.types import ArrayType, DoubleType, FloatType

from config.data import (SPARK_NAME,
                         DATA_DIR,
                         RAW_DIR,
                         TRAIN_DIR,
                         BG_NAME,
                         BG_TABLE,
                         BG_SCHEMA,
                         BR_NAME,
                         BR_TABLE,
                         BR_SCHEMA,
                         SR_NAME,
                         SR_TABLE,
                         SR_SCHEMA,
                         PH_NAME,
                         PH_TABLE,
                         PH_SCHEMA,
                         CSR_DIR,
                         EMB_NAME,
                         EMB_TABLE,
                         )
from config.train import DATA_PERIODS, TRAIN_NAME, TEST_NAME
from lib.graph import str_to_latlong
from sql.train import (BR_COL_SUBQUERY, 
                       EFFECTIVE_SUBQUERY, 
                       SR_BR_QUERY, 
                       ML_QUERY)


def cos_sim(a, b):
    return float(np.dot(a, b) / (np.linalg.norm(a) * np.linalg.norm(b)))


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
        udf_cos_sim = self.spark.udf.register("cos_sim", cos_sim, FloatType())
        geo_udf = self.spark.udf.register("str_to_latlong", lambda x: str_to_latlong(x), ArrayType(DoubleType()))
        
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

        self.ph_data = self.spark.read.csv(
            os.path.join(self.data_dir, RAW_DIR, PH_NAME),
            schema=PH_SCHEMA,
            header=True,
            dateFormat='yyyy-MM-dd',
        )
        self.ph_data.createOrReplaceTempView(PH_TABLE)

        self.emb_data = self.spark.read.parquet(
            os.path.join(self.data_dir, CSR_DIR, EMB_NAME),
        )
        self.emb_data.createOrReplaceTempView(EMB_TABLE)

    def write_ml(self):
        for key, val in DATA_PERIODS.items():
            _subqueries = []
            for col in ['StartTime', 'EndTime', 'Duration', 'EffectiveOnPH', 'DisabilityExt', 'Exemption', 'TypeDesc']:  # 
                _subqueries.append(
                    BR_COL_SUBQUERY.format(
                        effective_subquery=''.join([EFFECTIVE_SUBQUERY.format(n=n, target_col=col) for n in range(1, 7)]),
                        target_col=col
                    )
                )

            _query = SR_BR_QUERY.format(
                start_date=val['start'],
                end_date=val['end'],
                subqueries=''.join(_subqueries),
            )

            sr_br_data = self.query(_query, df=False)
            sr_br_data.createOrReplaceTempView(key)

            self.query(ML_QUERY.format(sr_br_table=key), df=False)\
                .write.mode('overwrite').parquet(os.path.join(DATA_DIR, TRAIN_DIR, f'{key}.parquet'))
        
        self.load_ml()
    
    def load_ml(self):
        self.train_data = self.spark.read.parquet(
            os.path.join(self.data_dir, TRAIN_DIR, f'{TRAIN_NAME}.parquet'),
        )
        self.train_data.createOrReplaceTempView(TRAIN_NAME)

        self.test_data = self.spark.read.parquet(
            os.path.join(self.data_dir, TRAIN_DIR, f'{TEST_NAME}.parquet'),
        )
        self.test_data.createOrReplaceTempView(TEST_NAME)
