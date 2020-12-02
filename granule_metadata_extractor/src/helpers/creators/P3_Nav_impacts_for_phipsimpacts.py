from datetime import datetime, timedelta
import os
import json

file_path = '/ftp/ops/public/pub/fieldCampaigns/impacts/P3_Nav/data'

nav_time = []
nav_lat = []
nav_lon = []

count = 0
for filename in os.listdir(file_path):
    nav_file = open(r'%s/%s'%(file_path,filename),'r')
    lines = nav_file.readlines()
    num_header_lines = int(lines[0].split(',')[0])
    print(filename,' ',len(lines)-num_header_lines)
    count = count + len(lines)-num_header_lines
     
    for i in range(num_header_lines,len(lines)):
        tkn = lines[i].split(',') #seconds since midnight, day_of_year, latitude, longitude
        time = datetime.strptime(r'%s%03d'%('2020',int(tkn[1])),'%Y%j')+timedelta(seconds=int(tkn[0]))
        nav_time.append(datetime.strftime(time,'%Y%m%d%H%M%S'))
        nav_lat.append(float(tkn[2]))
        nav_lon.append(float(tkn[3]))

    nav_file.close()
P3_Nav = {'time':nav_time,'lat':nav_lat,'lon':nav_lon}

with open('./P3_Nav_impacts.json', 'w') as fp:
    json.dump(P3_Nav, fp)
