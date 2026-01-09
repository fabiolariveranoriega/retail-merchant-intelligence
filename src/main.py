import argparse

from .data import EventData
from .clickhouse import ClickHouseClient


def main(args:dict):
    event_data = EventData(args["path"])
    event_data.preprocess()
    event_data.compute_metrics()

    client = ClickHouseClient(args["table"], args["host"], args["port"], args["user"], args["password"])
    client.load(event_data.get_data())

    event_data.to_csv(args["save_path"])

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Load data through clickhouse")

    parser.add_argument("--path", required=True)
    parser.add_argument("--table", required=True)
    parser.add_argument("--host", default="127.0.0.1", required=False)
    parser.add_argument("--port", default=9000, required=False)
    parser.add_argument("--user", default="admin", required=False)
    parser.add_argument("--password", default="password", required=False)
    parser.add_argument("--save_path", default="data/product_metrics_daily.csv", required=False)

    args = vars(parser.parse_args())

    main(args)


