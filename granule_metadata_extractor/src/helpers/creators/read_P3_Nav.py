from datetime import datetime, timedelta
import os
import json

file_path = '/ftp/ops/public/pub/fieldCampaigns/impacts/P3_Nav/data'

nav_time = []
nav_lat = []
nav_lon = []

count = 0
f0 = []
for filename in os.listdir(file_path):
    if 'P3B_2020' in filename:
       data_year = '2020'
    elif 'P3B_2022' in filename:
       data_year = '2022'

    nav_file = open(r'%s/%s'%(file_path,filename),'r')
    lines = nav_file.readlines()
    num_header_lines = int(lines[0].split(',')[0])
    print(filename,' ',len(lines)-num_header_lines,' ',data_year)
    count = count + len(lines)-num_header_lines
     
    for i in range(num_header_lines,len(lines)):
      if len(lines[i]) > 1:
        tkn = lines[i].split(',') #seconds since midnight, day_of_year, latitude, longitude
        if '-9999' in tkn[2] or '-9999' in tkn[3]:
           if len(f0) == 0:
              f0.append(filename)
           else:
              if filename not in f0:
                 f0.append(filename)
        else:
           time = datetime.strptime(r'%s%03d'%(data_year,int(tkn[1])),'%Y%j')+timedelta(seconds=int(tkn[0]))
           nav_time.append(datetime.strftime(time,'%Y%m%d%H%M%S'))
           nav_lat.append(float(tkn[2]))
           nav_lon.append(float(tkn[3]))

    nav_file.close()
P3_Nav = {'time':nav_time,'lat':nav_lat,'lon':nav_lon}

with open('./P3_Nav_impacts.json', 'w') as fp:
    json.dump(P3_Nav, fp)

print(f0)
