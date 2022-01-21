import datetime
import os
import time

import pymongo
from pymongo import DeleteMany
from collections import Counter

MONGO_DSN = os.environ["MONGO_DSN"]
connection = pymongo.MongoClient(MONGO_DSN,connect=False)


def duplicate_to_remove(entities: list, unique_id: str):
    count = Counter([i[f"{unique_id}"] for i in entities])
    duplicates = [item for item, c in count.items() if c > 1]
    print(f"Duplicates with {unique_id} to remove: {len(duplicates)}")


def remove_old_data() -> None:
    streams_interval = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(hours=1)
    print(f"streams_interval: {streams_interval}")
    streamers_interval = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(days=2)
    print(f"streamers_interval: {streamers_interval}")
    print("removing old streams")
    connection.db.stream_crawler.stream.delete_many({"updated_at": {"$lt": str(streams_interval)}})
    print("removing old streamers")
    connection.db.stream_crawler.streamer.delete_many({"updated_at": {"$lt": str(streamers_interval)}})


def generate_removing(entitys: list, unique_id: str) -> list:
    return [
        DeleteMany(
            {"_id": {"$gt": i["_id"]}, f"{unique_id}": i[f"{unique_id}"]}
        )
        for i in entitys
    ]


def run() -> None:
    remove_old_data()
    stream_res = connection.db.stream_crawler.stream.find({}, {"stream_id": 1}).sort([("_id", 1)])
    streamer_res = connection.db.stream_crawler.streamer.find({}, {"id": 1}).sort([("_id", 1)])
    streams = list(stream_res)
    streamers = list(streamer_res)
    duplicate_to_remove(streams, "stream_id")
    duplicate_to_remove(streamers, "id")
    print("trying to remove duplicates for streams")
    connection.db.stream_crawler.stream.bulk_write(generate_removing(streams, "stream_id"))
    print("removed successfully")
    print("trying to remove duplicates for streamers")
    connection.db.stream_crawler.streamer.bulk_write(generate_removing(streamers, "id"))
    print("removed successfully")


try:
    while True:
        run()
        time.sleep(1)
except KeyboardInterrupt:
    connection.close()
