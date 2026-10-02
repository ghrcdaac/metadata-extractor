"""Lookup creator for METNAV NURTURE (metnavnurture)."""
from datetime import datetime, time, timedelta, timezone
from utils.mdx import MDX
from utils.ames import open_icartt_1001
from typing import Any
from pathlib import Path
from zipfile import ZipFile
import os
import json

short_name = "metnavnurture"
provider_path = "metnavnurture/"

class MDXProcessing(MDX):

    def __init__(self):
        super().__init__()
        self.lookup = {}

    def main(self):
        # In creating initial lookup data, MDX process should be run twice.
        # Once extracts info from ICARTT data and places in lookup.
        # The second time, metadata for KML files can be drawn from ICARTT metadata.
        self.process_collection(short_name, provider_path)
        self.shutdown_ec2()

    def process(self, filename: str, stream=None) -> dict[str, Any]:
        if filename.lower().endswith(".ict"):
            # Process as ICARTT
            return self.process_icartt(filename, stream)
        elif filename.lower().endswith(".kml"):
            # Do lookup
            if filename in self.lookup:
                metadata = self.lookup[filename]
                del metadata['format']
                del metadata['sizeMB']
                for field in ['north', 'south', 'east', 'west']:
                    metadata[field] = float(metadata[field])
                for field in ['start', 'end']:
                    metadata[field] = datetime.fromisoformat(metadata[field])
                field['format'] = 'ASCII'
                return metadata
        else:
            return {}

    def process_icartt(self, filename: str, stream=None) -> dict[str, Any]:
        start_time = datetime.max.replace(tzinfo=timezone.utc)
        end_time = datetime.min.replace(tzinfo=timezone.utc)
        max_lon = float("-inf")
        min_lon = float("inf")
        max_lat = float("-inf")
        min_lat = float("inf")

        with open_icartt_1001(stream, encoding="utf-8") as (header, records):
            latitude_index = header.variable_names.index(
                "Latitude"
            )
            longitude_index = header.variable_names.index(
                "Longitude"
            )
            beginning_of_day = datetime.combine(
                header.data_date,
                time.min,
                tzinfo=timezone.utc,
            )

            for record in records:
                timestamp = beginning_of_day + timedelta(
                    seconds=record.independent
                )

                latitude = record.values[latitude_index]
                longitude = record.values[longitude_index]
                if not isinstance(latitude, float) or not isinstance(longitude, float):
                    continue
                if not -90 <= latitude <= 90 and -180 <= longitude <= 180:
                    continue

                min_lat = min(min_lat, latitude)
                max_lat = max(max_lat, latitude)
                min_lon = min(min_lon, longitude)
                max_lon = max(max_lon, longitude)
                start_time = min(start_time, timestamp)
                end_time = max(end_time, timestamp)

        return {
            "start": start_time,
            "end": end_time,
            "north": max_lat,
            "south": min_lat,
            "east": max_lon,
            "west": min_lon,
            "format": "ASCII"
        }

if __name__ == '__main__':
    MDXProcessing().main()
    # The below can be use to run a profiler and see which functions are
    # taking the most time to process
    # cProfile.run('MDXProcessing().main()', sort='tottime')