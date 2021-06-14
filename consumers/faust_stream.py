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
transform_topic_name = "org.chicago.cta.stations.table.v1"

app = faust.App("stations-stream", broker="kafka://localhost:9092", store="memory://")
ingested_topic = app.topic(ingested_topic_name, value_type=Station)
out_topic = app.topic("out_topic", partitions=1)
table = app.Table(
   "line",
   default=int,
   partitions=1,
   changelog_topic=out_topic,
)
transform_topic = app.topic(transform_topic_name, partitions=1)


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
        await transform_topic.send(key=key, value=transformer(station))


if __name__ == "__main__":
    app.main()
