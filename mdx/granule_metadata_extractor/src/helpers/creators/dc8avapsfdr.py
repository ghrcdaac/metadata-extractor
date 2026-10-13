"""Creator for DC-8 AVAPS Ferry."""

import re
from typing import Any
from datetime import datetime, timezone, timedelta, UTC
from netCDF4 import Dataset
import numpy as np

from utils.mdx import MDX
from utils.dropsonde import read_avaps_d_file, read_aspen_file

short_name = 'dc8avapsfdr'
provider_path = 'dc8avapsfdr/'


class MDXProcessing(MDX):
    def __init__(self):
        super().__init__()


    def process(self, filename: str, stream) -> dict[str, Any]:
        start_time = datetime.max.replace(tzinfo=timezone.utc)
        end_time = datetime.min.replace(tzinfo=timezone.utc)
        max_lon = float("-inf")
        min_lon = float("inf")
        max_lat = float("-inf")
        min_lat = float("inf")
        file_type = ""

        if re.search(r"_raw.txt$", filename):
            file_type = "ASCII"
            avaps_d = read_avaps_d_file(stream)
            for row in avaps_d.data:
                timestamp = row[0].item().replace(tzinfo=timezone.utc)
                start_time = min(start_time, timestamp)
                end_time = max(end_time, timestamp)

                lon = row[1]
                min_lon = min(min_lon, lon)
                max_lon = max(max_lon, lon)

                lat = row[2]
                min_lat = min(min_lat, lat)
                max_lat = max(max_lat, lat)

        elif re.search(r"_QC.txt$", filename):
            file_type = "ASCII"
            aspen = read_aspen_file(stream)
            for row in aspen.data:    
                timestamp = row[0].item().replace(tzinfo=timezone.utc)
                start_time = min(start_time, timestamp)
                end_time = max(end_time, timestamp)

                # QC data incorrectly flips sign on longitude, so this is corrected
                lon = row[1] * -1
                min_lon = min(min_lon, lon)
                max_lon = max(max_lon, lon)

                lat = row[2]
                min_lat = min(min_lat, lat)
                max_lat = max(max_lat, lat)

        else:
            file_type = "netCDF-3"
            nc = Dataset("in-mem-file", mode='r', memory=stream.read())
            # QC data incorrectly flips sign on longitude, so this is corrected
            lon = nc['lon'][:] * -1
            lat = nc['lat'][:]
            max_lat, min_lat, max_lon, min_lon = [np.nanmax(lat), np.nanmin(lat),
                                                  np.nanmax(lon), np.nanmin(lon)]

            base_time = datetime.fromtimestamp(nc['base_time'][0], UTC)
            timestamps = [base_time + timedelta(seconds=s) for s in nc['time_offset'][:]]
            start_time = min(timestamps)
            end_time = max(timestamps)

        return {
            "start": start_time,
            "end": end_time,
            "north": max_lat,
            "south": min_lat,
            "east": max_lon,
            "west": min_lon,
            "format": file_type
        }

    
    def main(self):
        self.process_collection(short_name, provider_path)
        self.shutdown_ec2()

        
if __name__ == '__main__':
    MDXProcessing().main()