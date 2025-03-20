# for all future collections
from datetime import datetime, timedelta
from utils.mdx import MDX
import cProfile
import time
import math
import re

short_name = "gpmpipuconn"
provider_path = "gpmpipuconn/2021_2022/SN_PIP003/2021/PIP_1/Raw_Video/00320210915/"
#file_type = "Binary"

#UConn_PIP_0022022122223400_a_p_60.pv2

instr_site = {'003':{'lat':41.808,'lon':-72.294},
              '002A':{'lat':41.808,'lon':-72.294}, #PI002 after 20231101
              '002B':{'lat':41.818,'lon':-72.258}, #PI002 before 20231101
             }
f_type = {'piv':'Binary','png':'PNG','dat':'ASCII'}
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
        self.file_type = "Binary"
        if filename.endswith('dat'):
            return self.read_metadata_ascii(filename,file_obj_stream)
        else:
            return self.read_metadata(filename)

    def read_metadata(self,filename):
        print(filename)
        fn = filename.split('/')[-1]
        self.file_type = f_type[fn.split('.')[-1]] #i.e., 'piv':'Binary'
        if fn.endswith('dat'):
           return self.read_metadata_ascii(filename, file_obj_stream)
        else:
           tkn = fn.split('.')[0].split('_')
           tmp = [x for x in tkn if len(x) > 12] #i.e., 0032021091515190
           utc_str = tmp[0]
           if utc_str.startswith('003'):
               lat = instr_site['003']['lat']
               lon = instr_site['003']['lon']
               start_time = datetime.strptime(utc_str,'003%Y%m%d%H%M0')
               end_time = start_time + timedelta(minutes=10)
           elif utc_str.startswith('002'):
               start_time = datetime.strptime(utc_str,'002%Y%m%d%H%M0')
               end_time = start_time + timedelta(minutes=10)
               if end_time <= datetime(2023,11,1):
                   lat = instr_site['002B']['lat']
                   lon = instr_site['002B']['lon']
           else:
               print('Error:',filename)
               exit()

           north = lat + 0.01
           south = lat - 0.01
           east = lon + 0.01
           west = lon - 0.01    
           return {
               "start": start_time,
               "end": end_time,
               "north": north,
               "south": south,
               "east": east,
               "west": west,
               "format": self.file_type
           }



    def read_metadata_ascii(self, filename, file_obj_stream):
        """
        Extracts temporal and spatial metadata from the following files:
        """
        print(filename)
        utc = []
        lats = []
        lons = []
        for encoded_line in file_obj_stream.iter_lines():
            #print(encoded_line)
            line = encoded_line.decode("utf-8")
            #print(line)
            #Sample: IWG110hz,2017-06-01T13:46:32.101, 26.078796, -80.154362,,
            tkn = line.split(',')
            if len(tkn[2]) != 0 and len(tkn[3]) != 0:
               utc.append(datetime.strptime(tkn[1],'%Y-%m-%dT%H:%M:%S.%f'))
               lats.append(float(tkn[2]))
               lons.append(float(tkn[3]))

        start_time, end_time = [min(utc), max(utc)]
        north, south, east, west = [max(lats),min(lats),max(lons),min(lons)]  

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
