# Create lookup zip for dc8avapstc4 
# for all future collections
from datetime import datetime, timedelta
from utils.mdx import MDX
import cProfile
import time
import re

short_name = "dc8avapstc4"
provider_path = "dc8avapstc4/"

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
        for line in file_lines[num_of_header_lines:]:
            tkn = line.split()
            if tkn[14] != '9e+09' and tkn[15] != '9e+09':
               lat0.append(float(tkn[15]))
               lon0.append(float(tkn[14]))
               sec0.append(float(tkn[0]))
        launch_time_string =list(filter(lambda x: 'UTC Launch Time (y,m,d,h,m,s):' in x, file_lines))[0]
        utc_date = datetime.strptime(''.join(launch_time_string.split()[4:7]),'%Y,%m,%d,')
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
