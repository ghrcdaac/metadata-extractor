# for all future collections
from datetime import datetime, timedelta
from utils.mdx import MDX
import cProfile
import time
import math
import re

short_name = "gpmsmasuconn"
provider_path = "gpmsmasuconn/"
file_type = "BMP" #browse image

site_lat = 41.808
site_lon = -72.294


class MDXProcessing(MDX):

    def __init__(self):
        super().__init__()

    def process(self, filename, file_obj_stream) -> dict:
        """
        Individual collection processing logic for spatial and temporal 
        metadata extraction
        :param filename: name of file to process
        :type filename: str
        """
        return self.read_metadata(filename)

    def read_metadata(self,filename):
        """
        Extracts temporal and spatial metadata orom browse files
        """
        #UConn_SMAS_20230114193150000CAM0_1.bmp
        start_time = datetime.strptime(filename.split('_')[2][0:14],'%Y%m%d%H%M%S')
        end_time = start_time 
        north, south, east, west = [site_lat + 0.001, site_lat - 0.001,
                                    site_lon + 0.001, site_lon - 0.001]
        return {
            "start": start_time,
            "end": end_time,
            "north": north,
            "south": south,
            "east": east,
            "west": west,
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
