from urllib.parse import urlparse 
from datetime import datetime
import concurrent.futures
import subprocess
import hashlib
import zipfile 
import boto3
import json
import os   
import time

from zipfile import ZipFile

bucket="ghrcw-private"
shortname = "navaloft"

nav_time = []
nav_lat = []
nav_lon = []


class S3URI:
    def __init__(self, bucket, prefix, filename):
        self.bucket = bucket
        self.prefix = prefix
        self.filename = filename


#main
s3_client = boto3.client('s3')
paginator = s3_client.get_paginator('list_objects_v2')
page_iterator = paginator.paginate(Bucket=bucket,Prefix=shortname,PaginationConfig = {'PageSize': 1000})
for page in page_iterator:
    for obj in page["Contents"]:
        obj_key=obj['Key']
        s3uri = f"s3://{bucket}/{obj_key}"
        #'s3://ghrcw-private/navaloft/ALOFT_Nav_IWG1_31Jul2023-1753'
        parsed_s3uri = urlparse(s3uri, allow_fragments=False)
        uri = S3URI(parsed_s3uri.netloc, parsed_s3uri.path.lstrip('/'),
                 os.path.basename(s3uri))
        # Download file object stream and size
        response = s3_client.get_object(Bucket=uri.bucket, Key=uri.prefix)
        file_obj_stream = response["Body"]
    
        # Extracts temporal and spatial metadata from the following files:
        for encoded_line in file_obj_stream.iter_lines():
            line = encoded_line.decode("utf-8")
            tkn = line.split(',')
            if len(tkn[2]) != 0 and len(tkn[3]) != 0:
               time = datetime.strptime(tkn[1],'%Y-%m-%dT%H:%M:%S.%f')
               nav_time.append(datetime.strftime(time,'%Y%m%d%H%M%S'))
               nav_lat.append(float(tkn[2]))
               nav_lon.append(float(tkn[3]))
               #print(datetime.strftime(time,'%Y%m%d%H%M%S'),tkn[2],tkn[3])

from operator import itemgetter
indices, time_sorted = zip(*sorted(enumerate(nav_time), key=itemgetter(1)))
lat_sorted = [nav_lat[i] for i in indices]
lon_sorted = [nav_lon[i] for i in indices]

nav_time = time_sorted
nav_lat = lat_sorted
nav_lon = lon_sorted

Nav_aloft = {'time':nav_time,'lat':nav_lat,'lon':nav_lon}

with open('./Nav_aloft.json', 'w') as fp:
    json.dump(Nav_aloft, fp)

# The below 2 line can also be substituted by the command line "zip Nav_aloft.zip Nav_aloft.json"
with ZipFile('./Nav_aloft.zip', 'w') as myzip:
    myzip.write('Nav_aloft.json')

