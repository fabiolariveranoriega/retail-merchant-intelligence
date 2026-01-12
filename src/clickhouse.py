import pandas as pd
from clickhouse_driver import Client
from datetime import date

from abc import ABC, abstractmethod


class BaseClient(ABC):
    def __init__(self):
        super().__init__()

    @abstractmethod
    def load(self, data:pd.DataFrame):
        raise NotImplementedError("load not implemented.")
    
class ClickHouseClient(BaseClient):
    def __init__(self, table:str = "merchant_intelligence.events", host:str = '127.0.0.1', port:int = 9000, 
        user:str = 'admin', password:str = 'password'):
        super().__init__()

        
        self.client = Client(
            host=host,
            port=port,
            user=user,
            password=password
        )
        self.table = table

    def load(self, data:pd.DataFrame):
        
        
        data_to_insert = [tuple(x) for x in data.to_numpy()]

        
        self.client.execute(
            f"INSERT INTO {self.table} VALUES",
            data_to_insert
        )

        
        rows = self.client.execute(f"SELECT * FROM {self.table} LIMIT 5")
        print(rows)


