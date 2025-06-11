# for all future collections
from datetime import datetime, timedelta
from utils.mdx import MDX
import cProfile
import time

short_name = "gpmmascuconn"
provider_path = "gpmmascuconn/"
file_type = "PNG"

class MDXProcessing(MDX):

    def __init__(self):
        super().__init__()
        self.lat = 41.808
        self.lon = -72.294

    def process(self, filename, file_obj_stream) -> dict:
        """
        Individual collection processing logic for spatial and temporal 
        metadata extraction
        :param filename: name of file to process
        :type filename: str
        :param file_obj_stream: file object stream to be processed
        :type file_obj_stream: botocore.response.StreamingBody
        """
        return self.read_metadata_png(filename)


    def read_metadata_png(self, filename):
        """
        Extracts temporal and spatial metadata from the following files:
        """
        print(filename)
        #Sample file: UConn_MASC_2023_01_14_19_36_16_flake_885_cam_0.png
        tkn = filename.split('/')[-1].split('_')
        start_time = datetime.strptime(''.join(tkn[2:8]),'%Y%m%d%H%M%S')        
        end_time = start_time
        north, south, east, west = [self.lat + 0.001, self.lat - 0.001,
                                    self.lon + 0.001, self.lon - 0.001]  

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
