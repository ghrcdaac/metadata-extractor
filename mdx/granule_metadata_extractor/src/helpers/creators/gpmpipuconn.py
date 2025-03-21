# for all future collections
from datetime import datetime, timedelta
from utils.mdx import MDX
import cProfile
import time
import math
import re

short_name = "gpmpipuconn"
#provider_path = "gpmpipuconn/2021_2022/SN_PIP003/2021/PIP_1/Raw_Video/00320210915/"
#provider_path = "gpmpipuconn/2021_2022/SN_PIP003/2021/PIP_2/a_Particle_Tables/00320211102/"
#provider_path = "gpmpipuconn/2021_2022/SN_PIP003/2021/PIP_2/b_Particle_Tables/00320211028/"
#provider_path = "gpmpipuconn/2021_2022/SN_PIP003/2021/PIP_3/f_10_Summary/PIP_log/"
#provider_path = "gpmpipuconn/2021_2022/SN_PIP003/2021/PIP_3/f_1_1_Particle_Tables_subsample/00320211030/"
#provider_path = "gpmpipuconn/2021_2022/SN_PIP003/2021/PIP_3/f_1_2_Particle_Tables_ascii/00320211207/"
#provider_path = "gpmpipuconn/2021_2022/SN_PIP003/2021/PIP_3/f_2_1_Tracker_Tables/00320211018/" #i.e.,UConn_PIP_0032021101822500_a_t.dat
#provider_path = "gpmpipuconn/2021_2022/SN_PIP003/2021/PIP_3/f_2_2_Velocity_Tables/00320211119/" #i.e.,UConn_PIP_0032021111902020_a_v_1.dat
#provider_path = "gpmpipuconn/2021_2022/SN_PIP003/2021/PIP_3/f_2_3_0_Vel_Scat/00320211215/" #i.e.,UConn_PIP_0032021121523500_a_v_1_2.png
#provider_path = "gpmpipuconn/2021_2022/SN_PIP003/2021/PIP_3/f_2_3_1_Vel_Ebar/00320211202/" #i.e.,UConn_PIP_0032021120213200_V_Ebar.png
provider_path = "gpmpipuconn/2021_2022/SN_PIP003/2021/"


instr_site = {'003':{'lat':41.808,'lon':-72.294},
              '002A':{'lat':41.818,'lon':-72.258}, #PI002 before 20231101
              '002B':{'lat':41.808,'lon':-72.294}, #PI002 after 20231101
             }
f_type = {'piv':'Binary','pv2':'Binary','png':'PNG','dat':'ASCII','log':'ASCII'}
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
        fn = filename.split('/')[-1]
        self.file_type = f_type[fn.split('.')[-1]] #i.e., 'piv':'Binary', or 'pv2':'Binary'
        tkn = fn.split('.')[0].split('_')
        tmp = [x for x in tkn if len(x) > 12] #i.e., 0032021091515190 (piv or pv2) or 20210915T155229 (log)

        if filename.endswith('piv') or filename.endswith('pv2') or filename.endswith('png'): #Raw_Video
           utc_str = tmp[0]
           if utc_str.startswith('003'):
               site_key = '003'
               start_time = datetime.strptime(utc_str,'003%Y%m%d%H%M0')
               end_time = start_time + timedelta(seconds=599)
           elif utc_str.startswith('002'):
               start_time = datetime.strptime(utc_str,'002%Y%m%d%H%M0')
               end_time = start_time + timedelta(seconds=599) #10 minutes
               if end_time <= datetime(2023,11,1):
                   site_key = '002A'
               else:    
                   site_key = '002B'

        elif filename.endswith('log'):
           #UConn_PIP_PIP003_IMP_20210915T155229.log
           start_time = datetime.strptime(tmp[0],'%Y%m%dT%H%M%S')
           end_time = datetime.strptime(start_time.strftime('%Y%m%d'),'%Y%m%d')+timedelta(seconds=3600*24-1)
           if 'PIP003' in tkn:
               site_key = '003'
           else: #'PIP002
               if end_time <= datetime(2023,11,1):
                  site_key = '002A'
               else:
                  site_key = '002B' 
        lat = instr_site[site_key]['lat']
        lon = instr_site[site_key]['lon']

        north = lat + 0.01
        south = lat - 0.01
        east = lon + 0.01
        west = lon - 0.01    
        print(filename,start_time,end_time,north,south,east,west)
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
            print(filename,len(tkn))
            if tkn[0] != '-99' and len(tkn) >13:
               utc_char = [tkn[6].zfill(4),tkn[7].zfill(2),tkn[8].zfill(2),tkn[9].zfill(2),tkn[10].zfill(2),tkn[11].zfill(2)]
               utc_str = ''.join(utc_char)
               utc.append(datetime.strptime(utc_str,'%Y%m%d%H%M%S'))
        if len(utc) == 0:
           if tmp[0].startswith('003'):
               start_time = datetime.strptime(tmp[0],'003%Y%m%d%H%M0')
           elif tmp[0].startswith('002'):
               start_time = datetime.strptime(tmp[0],'002%Y%m%d%H%M0')
           end_time = start_time + timedelta(seconds=599) #10 minutes
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

        print(filename, start_time, end_time, north, south, east, west)
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
