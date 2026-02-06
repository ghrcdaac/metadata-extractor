# Create lookup zip for sondewhymsie 
# for all future collections
from datetime import datetime, timedelta
from utils.mdx import MDX
import cProfile
import time
import re

short_name = "sondewhymsie"
provider_path = "sondewhymsie/"

class MDXProcessing(MDX):

    def __init__(self):
        super().__init__()
        self.file_type = "ASCII"

    def process(self, filename, file_obj_stream) -> dict:
        """
        Individual collection processing logic for spatial and temporal 
        metadata extraction
        :param filename: name of file to process
        :type filename: str
        :param file_obj_stream: file object stream to be processed
        :type file_obj_stream: botocore.response.StreamingBody
        """
        self.file_type = "ASCII"
        return self.read_metadata_ascii(filename, file_obj_stream)


    def read_metadata_ascii(self,filename, file_obj_stream):
        """
        Extract temporal and spatial metadata from ascii files
        """
        file_lines = []
        for encoded_line in file_obj_stream.iter_lines():
            err_flag = 0
            try:
                decoded_line = encoded_line.decode("utf-8")
            except:
                err_flag = 1

            if err_flag == 0:
                file_lines.append(decoded_line)

        launch_loc_string =list(filter(lambda x: 'LOCATION:' in x, file_lines))[0]
        tkn = launch_loc_string.split()
        lat = tkn[-3]
        lon = tkn[-1]

        #whymsie_radiosondes_AlbuquerqueNWS_20241022_R0_L1.ict
        utc_date_str = filename.split('_')[3] #i.e., 20241022
        launch_time_string =list(filter(lambda x: 'ASSOCIATED_DATA: Launch time' in x, file_lines))[0]
        utc_time_str = launch_time_string.split()[-2]  #i.e., '21:00'
        minTime = datetime.strptime(utc_date_str+utc_time_str,'%Y%m%d%H:%M')
        #The balloon typically rises at a speed of 1,000 feet per minute. 
        #It takes about two hours to reach a float altitude of 120,000 feet.
        maxTime = minTime+timedelta(hours=2)
        north, south, east, west = [lat, lat, lon, lon]

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
