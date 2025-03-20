# for all future collections
from datetime import datetime, timedelta
from utils.mdx import MDX
import cProfile
import time
import math
import re

short_name = "gpmpipuconn"
#provider_path = "gpmpipuconn/2021_2022/SN_PIP003/2021/PIP_1/Raw_Video/00320210915/"
provider_path = "gpmpipuconn/2021_2022/SN_PIP003/2021/PIP_2/a_Particle_Tables/00320211102/"

#UConn_PIP_0022022122223400_a_p_60.pv2

instr_site = {'003':{'lat':41.808,'lon':-72.294},
              '002A':{'lat':41.818,'lon':-72.258}, #PI002 before 20231101
              '002B':{'lat':41.808,'lon':-72.294}, #PI002 after 20231101
             }
f_type = {'piv':'Binary','pv2':'Binary','png':'PNG','dat':'ASCII'}
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
        if filename.endswith('piv'): #Raw_Video
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
                   lat = instr_site['002A']['lat']
                   lon = instr_site['002A']['lon']
               else:    
                   lat = instr_site['002B']['lat']
                   lon = instr_site['002B']['lon']
           else:
               print('Error:',filename)
               exit()

        north = lat + 0.01
        south = lat - 0.01
        east = lon + 0.01
        west = lon - 0.01    
        print(start_time,end_time,north,south,east,west)
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
        #Sample file: UConn_PIP_0032021110221400_a_p.dat
        fn = filename.split('/')[-1]
        self.file_type = f_type[fn.split('.')[-1]] #i.e., 'dat':'ASCII'
        tkn = fn.split('.')[0].split('_')
        tmp = [x for x in tkn if len(x) > 12] #i.e., 0032021091515190

        lines = []
        for encoded_line in file_obj_stream.iter_lines():
            lines.append(encoded_line.decode("utf-8"))
        utc = []    
        for line in lines[10:]:
            tkn = line.split()
            if tkn[0] != '-99':
               utc_str = [x.zfill(2) for x in tkn[7:12]].insert(0,tkn[6].zfill(4))
               utc.append(datetime.strptime(utc_str,'%Y%m%d%H%M%S'))

        if len(utc) == 0:
           if tmp[0].startswith('003'):
               start_time = datetime.strptime(tmp[0],'003%Y%m%d%H%M0')
           elif tmp[0].startswith('002'):
               start_time = datetime.strptime(tmp[0],'002%Y%m%d%H%M0')
           end_time = start_time + timedelta(minutes=10)
        else:    
           start_time, end_time = [min(utc), max(utc)]

        if tmp[0].startswith('003'):
           lat = instr_site['003']['lat']
           lon = instr_site['003']['lon']
        elif tmp[0].startswith('002'):
           if end_time <= datetime(2023,11,1):
              lat = instr_site['002A']['lat']
              lon = instr_site['002A']['lon']
           else:
              lat = instr_site['002B']['lat']
              lon = instr_site['002B']['lon']

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
