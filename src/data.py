from abc import ABC, abstractmethod
import pandas as pd

class Data(ABC):
    def __init__(self):
        super().__init__()

    @abstractmethod
    def preprocess(self):
        raise NotImplementedError("preprocess_data not implemented.")
    
    @abstractmethod
    def compute_metrics(self):
        raise NotImplementedError("compute_metrics not implemented.")
    
    @abstractmethod
    def to_csv(self, data_path:str):
        raise NotImplementedError("to_csv not implemented.")
    
    @abstractmethod
    def get_data(self):
        raise NotImplementedError("get_data not implemented.")
    
class EventData(Data):
    def __init__(self, data_path):
        super().__init__()
        self.events = pd.read_csv(data_path)

    def preprocess(self):
        self.events = self.events[self.events['event'].isin(['view', 'addtocart', 'transaction'])]

        self.events['timestamp'] = pd.to_datetime(self.events['timestamp'], unit='ms')

        # create product id col. as type str to prevent any problems with querying (int vs str)
        self.events['product_id'] = self.events['itemid'].astype(str)

    def compute_metrics(self):
        self.events['date'] = self.events['timestamp'].dt.date

        # counts how many views, add-to-carts, and transactions happened for each product on each day
        # converts event level like 'view', 'addtocart', and 'transaction' into separate cols. 
        # if a product has no events of a certain type, pandas fills it with 0. 
        metrics = self.events.groupby(['date', 'product_id', 'event']).size().unstack(fill_value=0)

        metrics['impressions'] = metrics['view']
        metrics['clicks'] = metrics['addtocart'] + metrics['transaction']

        # click through rate = (add to cart + transaction) / view
        metrics['ctr'] = metrics['clicks'] / metrics['impressions'].replace(0,1) # prevents division by 0 if prod had 0 impressions
        
        metrics.reset_index()


    def to_csv(self, data_path:str):
        self.events.to_csv(data_path, index=False)

    def get_data(self):
        return self.events
