import datetime
import os
import time
import logging

import pymongo
from pymongo import DeleteMany
from collections import Counter

MONGO_DSN = os.environ["MONGO_DSN"]
connection = pymongo.MongoClient(MONGO_DSN, connect=False)
logging.basicConfig(encoding='utf-8', level=logging.DEBUG,format='%(asctime)s %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p')


def duplicate_to_remove(entities: list, unique_id: str):
    count = Counter([i[f"{unique_id}"] for i in entities])
    duplicates = [item for item, c in count.items() if c > 1]
    logging.debug(f"Duplicates with {unique_id} to remove: {len(duplicates)}")


def remove_old_data() -> None:
    streams_interval = datetime.datetime.utcnow() - datetime.timedelta(hours=1)
    logging.debug(f"streams_interval: {streams_interval}")
    streamers_interval = datetime.datetime.utcnow() - datetime.timedelta(days=1)
    logging.debug(f"streamers_interval: {streamers_interval}")
    logging.debug("removing old streams")
    connection.db.stream_crawler.stream.delete_many(
        {"updated_at": {"$lt": streams_interval.strftime('%Y-%m-%dT%H:%M:%S.%f%z')}}
    )
    logging.debug("removing old streamers")
    connection.db.stream_crawler.streamer.delete_many(
        {"updated_at": {"$lt": streamers_interval.strftime('%Y-%m-%dT%H:%M:%S.%f%z')}}
    )


def generate_removing(entitys: list, unique_id: str) -> list:
    return [
        DeleteMany(
            {"_id": {"$gt": i["_id"]}, f"{unique_id}": i[f"{unique_id}"]}
        )
        for i in entitys
    ]


def get_streams() -> list:
    stream_res = connection.db.stream_crawler.stream.find({}, {"stream_id": 1}).sort([("_id", 1)])
    return list(stream_res)


def get_streamers() -> list:
    streamer_res = connection.db.stream_crawler.streamer.find({}, {"id": 1}).sort([("_id", 1)])
    return list(streamer_res)


def run() -> None:
    remove_old_data()
    streams = get_streams()
    if streams:
        duplicate_to_remove(streams, "stream_id")
    streamers = get_streamers()
    if streamers:
        duplicate_to_remove(streamers, "id")

    streams_to_remove = generate_removing(streams, "stream_id")
    logging.debug("trying to remove duplicates for streams")
    if streams_to_remove:
        connection.db.stream_crawler.stream.bulk_write(streams_to_remove)
        logging.debug("removed successfully")

    streamers_to_remove = generate_removing(streamers, "id")

    logging.debug("trying to remove duplicates for streamers")
    if streamers_to_remove:
        connection.db.stream_crawler.streamer.bulk_write(streamers_to_remove)
        logging.debug("removed successfully")


try:
    while True:
        run()
        time.sleep(1)
except KeyboardInterrupt:
    connection.close()
