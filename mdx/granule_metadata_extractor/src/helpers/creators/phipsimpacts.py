# create lookup zip for phipsimpacts
# for all future collections
from datetime import datetime, timedelta
from utils.mdx import MDX
import cProfile
import time
import math
import re

short_name = "phipsimpacts"
provider_path = "phipsimpacts/fieldCampaigns/impacts/PHIPS/browse/"
file_type = "PNG"

class MDXProcessing(MDX):

    def __init__(self):
        super().__init__()
        gps_path = '../P3_Nav_impacts.zip'
        with ZipFile(os.path.join(os.path.dirname(__file__),gps_path), 'r') as p3zip:
            with p3zip.open('P3_Nav_impacts.json') as p3object:
                P3_Nav = json.load(p3object)
        self.nav_time = np.array([datetime.strptime(x,'%Y%m%d%H%M%S') for x in P3_Nav['time'][:]])
        self.nav_lat = np.array(P3_Nav['lat'][:])
        self.nav_lon = np.array(P3_Nav['lon'][:])

    def process(self, filename, file_obj_stream) -> dict:
        """
        Individual collection processing logic for spatial and temporal 
        metadata extraction
        :param filename: name of file to process
        :type filename: str
        :param file_obj_stream: file object stream to be processed
        :type file_obj_stream: botocore.response.StreamingBody
        """
        return self.read_metadata_regular(filename, file_obj_stream)


    def read_metadata_regular(self, filename, file_obj_stream):
        """
        Extracts temporal metadata from the file name:
        Extract spatial metadata from P3_Nav_impacts.zip
        """
        #sample file:
        #IMPACTS_PHIPS_20200118_1723_20200119000055_037826_c1.png
        tkn = filename.split('/')[-1].split('_')
        minTime = datetime.strptime(''.join(tkn[2:4]),'%Y%m%d%H%M')
        maxTime = datetime.strptime(tkn[4],'%Y%m%d%H%M%S')

        idx = np.where((self.nav_time >= minTime) & (self.nav_time <= maxTime))[0]
        maxlat, minlat, maxlon, minlon = [self.nav_lat[idx].max(),
                                          self.nav_lat[idx].min(),
                                          self.nav_lon[idx].max(),
                                          self.nav_lon[idx].min()]
        return {
            "start": minTime,
            "end": maxtime,
            "north": maxlat,
            "south": minlat,
            "east": maxlon,
            "west": minlon,
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
