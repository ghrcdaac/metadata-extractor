from datetime import datetime, timedelta
from zipfile import ZipFile
import json
import os

file_path = '/ftp/ops/public/pub/fieldCampaigns/impacts/P3_Nav/data'

nav_time = []
nav_lat = []
nav_lon = []

for filename in os.listdir(file_path):
    nav_file = open(r'%s/%s'%(file_path,filename),'r')
    lines = nav_file.readlines()
    num_header_lines = int(lines[0].split(',')[0])

    #IMPACTS_MetNav_P3B_20200112_R0.ict
    nav_year_str = filename.split('_')[3][0:4] #'2020' or '2022'
     
    for i in range(num_header_lines,len(lines)):
        tkn = lines[i].split(',') #seconds since midnight, day_of_year, latitude, longitude
        if len(tkn) > 1: 
           if tkn[2] != '-9999' and tkn[3] != '-9999':
              time = datetime.strptime(r'%s%03d'%(nav_year_str,int(tkn[1])),'%Y%j')+timedelta(seconds=int(tkn[0]))
              nav_time.append(datetime.strftime(time,'%Y%m%d%H%M%S'))
              nav_lat.append(float(tkn[2]))
              nav_lon.append(float(tkn[3]))

    nav_file.close()
P3_Nav = {'time':nav_time,'lat':nav_lat,'lon':nav_lon}

with open('./P3_Nav_impacts.json', 'w') as fp:
    json.dump(P3_Nav, fp)

# The below 2 line can also be substituted by the command line "zip P3_Nav_impacts.zip P3_Nav_impacts.json"
with ZipFile('./P3_Nav_impacts.zip', 'w') as myzip:
    myzip.write('P3_Nav_impacts.json')
