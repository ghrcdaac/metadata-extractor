# create lookup zip for glmmth 
# for all future collections
from datetime import datetime, timedelta
from utils.mdx import MDX
import cProfile
import time
import re

short_name = "glmmth"
provider_path = "glmmth/"

class MDXProcessing(MDX):

    def __init__(self):
        super().__init__()
        self.file_type = "netCDF-4"

    def process(self, filename, file_obj_stream) -> dict:
        """
        Individual collection processing logic for spatial and temporal 
        metadata extraction
        :param filename: name of file to process
        :type filename: str
        :param file_obj_stream: file object stream to be processed
        :type file_obj_stream: botocore.response.StreamingBody
        """
        return self.get_nc_metadata(filename)

    def get_nc_metadata(self,filename):
        #Sample file: GLMc_th_2019.nc
        utc_year_str = filename.split('/')[-1].split('.nc')[0].split('_')[2]
        utc_year = int(utc_year_str)
        start_time = datetime(utc_year,1,1)
        end_time = datetime(utc_year,12,31,23,59,59)

        north, south, east, west = [90., -90., 180., -180.]

        return {
            "start": start_time,
            "end": end_time,
            "north": north,
            "south": south,
            "east": east,
            "west": west,
            "format": self.file_type
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
