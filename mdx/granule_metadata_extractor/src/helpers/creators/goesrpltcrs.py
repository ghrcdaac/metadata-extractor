from datetime import datetime, timedelta
from netCDF4 import Dataset
from utils.mdx import MDX
import numpy as np
import cProfile
import time
import os

short_name = "goesrpltcrs"
provider_path = "goesrpltcrs/fieldCampaigns/goesrplt/CRS/data/"
file_type = "Not Provided"


class MDXProcessing(MDX):

    def __init__(self):
        super().__init__()

    def process(self, filename, file_obj_stream) -> dict:
        """
        Individual collection processing logic for spatial and temporal 
        metadata extraction
        :param filename: name of file to process
        :type filename: str
        :param file_obj_stream: file object stream to be processed
        :type file_obj_stream: botocore.response.StreamingBody
        """
        nc = Dataset("in-mem-file", mode='r', memory=file_obj_stream.read())
        lat = np.array(nc['lat'])
        lon = np.array(nc['lon'])
        time = np.array(nc['time'])
        date = os.path.basename(nc.filename).split('_')[3]
        file_datetime = datetime(int(date[:4]), int(date[4:6].lstrip('0')), int(date[6:].lstrip('0')))
        return {
            "start": file_datetime + timedelta(hours=float(min(time))),
            "end": file_datetime + timedelta(hours=float(max(time))),
            "north": max(lat),
            "south": min(lat),
            "east": max(lon),
            "west": min(lon),
            "format": file_type
        }
        

    def main(self):
        # start_time = time.time()
        self.process_collection(short_name, provider_path)
        # elapsed_time = time.time() - start_time
        # print(f"Elapsed time in seconds: {elapsed_time}")
        self.shutdown_ec2()


if __name__ == '__main__':
    MDXProcessing().main()
    # The below can be use to run a profiler and see which functions are
    # taking the most time to process
    # cProfile.run('MDXProcessing().main()', sort='tottime')
