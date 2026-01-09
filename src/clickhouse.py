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

        # Connect to ClickHouse
        self.client = Client(
            host=host,
            port=port,
            user=user,
            password=password
        )
        self.table = table

    def load(self, data:pd.DataFrame):
        # Convert to list of tuples
        data_to_insert = [tuple(x) for x in data.to_numpy()]

        # Insert into ClickHouse
        self.client.execute(
            f"INSERT INTO {self.table} VALUES",
            data_to_insert
        )

        # Verify
        rows = self.client.execute(f"SELECT * FROM {self.table} LIMIT 5")
        print(rows)


