# Create lookup zip for dc8navtc4 
# for all future collections
from datetime import datetime, timedelta
from utils.mdx import MDX
import cProfile
import time
import re

short_name = "dc8navtc4"
provider_path = "dc8navtc4/"

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
        self.file_type = 'ASCII'
        return self.read_metadata_ascii(filename, file_obj_stream)

    def read_metadata_ascii(self,filename, file_obj_stream):
        """
        Extract temporal and spatial metadata from ascii files
        """
        file_lines = []
        for encoded_line in file_obj_stream.iter_lines():
            file_lines.append(encoded_line.decode("utf-8"))

        num_of_header_lines = int(file_lines[0].split()[0])
        sec0 = []
        lat0 = []
        lon0 = []
        n_element = 0
        for line in file_lines[num_of_header_lines:]:
            tkn = line.split()
            if n_element == 0:
               if tkn[1] != '9e+09' and tkn[2] != '9e+09':
                  lat0.append(float(tkn[1]))
                  lon0.append(float(tkn[2]))
                  sec0.append(float(tkn[0]))
            n_element = n_element + len(tkn)
            if n_element >= 32:
               n_element = 0

        #TC4_Nav_DC8_TN20070724.txt
        #Extract date info from file name
        utc_date_string = filename.split('.')[0].split('_')[-1] #i.e., TN20070724
        utc_date = datetime.strptime(utc_date_string,'TN%Y%m%d')
        minTime = utc_date+timedelta(seconds=min(sec0))
        maxTime = utc_date+timedelta(seconds=max(sec0))
        north = max(lat0)
        south = min(lat0)
        west = min(lon0)
        east = max(lon0)

        return {
            "start": minTime,
            "end": maxTime,
            "north": north,
            "south": south,
            "east": east,
            "west": west,
            "format":self.file_type
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
