"""Defines trends calculations for stations"""
import logging

import faust

logger = logging.getLogger(__name__)


# Faust will ingest records from Kafka in this format
class Station(faust.Record):
    stop_id: int
    direction_id: str
    stop_name: str
    station_name: str
    station_descriptive_name: str
    station_id: int
    order: int
    red: bool
    blue: bool
    green: bool


# Faust will produce records to Kafka in this format
class TransformedStation(faust.Record):
    station_id: int
    station_name: str
    order: int
    line: str


ingested_topic_name = "jdbc_stations"
out_topic_name = "org.chicago.cta.stations.table.v1"

# TODO: Define a Faust Stream that ingests data from the Kafka Connect stations topic and
#   places it into a new topic with only the necessary information.
app = faust.App("stations-stream", broker="kafka://localhost:9092", store="memory://")
ingested_topic = app.topic(ingested_topic_name, value_type=Station)
out_topic = app.topic(out_topic_name, partitions=1)
# # TODO: Define a Faust Table
# table = app.Table(
#    # "TODO",
#    # default=TODO,
#    partitions=1,
#    changelog_topic=out_topic,
# )


def transformer(station: Station) -> TransformedStation:
    line = "red" if station.red else "blue" if station.blue else "green"
    return TransformedStation(
        station_id=station.station_id,
        station_name=station.station_name,
        order=station.order,
        line=line
    )


@app.agent(ingested_topic)
async def transform_stations(stations):
    async for key, station in stations.items():
        await out_topic.send(key=key, value=transformer(station))


if __name__ == "__main__":
    app.main()
