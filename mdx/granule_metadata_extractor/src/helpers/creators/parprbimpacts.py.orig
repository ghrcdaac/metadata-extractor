#-----------------------------------------------------------------------------#
# Assemble needed resources                                                   #
#-----------------------------------------------------------------------------#
import os, json, datetime
from datetime import datetime, date, timedelta
from netCDF4 import Dataset
import numpy as np
from hashlib import md5

#-- latest development prefer fix root directory
data_dir = "/ftp/ops/public/pub/fieldCampaigns/impacts/NCAR_Particle_Probes/data"

metadata = {}
metadata_ref = {}

def get_nc_metadata(filename):
    datafile = Dataset(filename)
    lats = np.array(datafile['LAT'][:])
    lons = np.array(datafile['LON'][:])
    sec = np.array(datafile['time'][:])
    ref_time = datetime.strptime(datafile.FlightDate,'%m/%d/%Y') 

    #Extract metadata from netCDF-4 data files
    start, end = [ref_time+timedelta(seconds=sec.min()),
                              ref_time+timedelta(seconds=sec.max())]
    nlat, slat, elon, wlon = [np.nanmax(lats),
                              np.nanmin(lats),
                              np.nanmax(lons),
                              np.nanmin(lons)]

    #create netCDF4 metadata for json input
    key0 = os.path.basename(filename)
    metadata[key0] = {}
    metadata[key0]["temporal"] = [x.strftime('%Y-%m-%dT%H:%M:%SZ') for x in [start,end]]
    metadata[key0]["wnes_geometry"] = [str(round(x,3)) for x in [wlon,nlat,elon,slat]]
    metadata[key0]['SizeMBDataGranule'] = str(round(1E-6 * os.path.getsize(filename), 2))
    with open(filename, 'rb') as file:
        metadata[key0]['checksum'] = md5(file.read()).hexdigest()
    metadata[key0]['format'] = 'netCDF-4'

    #collect metadata that will be assigned to PNG
    rr = {}
    key = filename.split('/')[-1].split('_')[2] #i.e., 20200108
    if key not in metadata_ref.keys():
       rr['start'], rr['end'] = [start,end]
       rr['NLat'], rr['SLat'], rr['ELon'], rr['WLon'] = [nlat,slat,elon,wlon] 
       rr['Probes'] = datafile.ProbeName
    else:
       #If metadata_ref[key] already exists,update values
       rr['start'] = min(metadata_ref[key]['start'],start)
       rr['end'] = max(metadata_ref[key]['end'],end)
       rr['NLat'] = max(metadata_ref[key]['NLat'],nlat)
       rr['SLat'] = min(metadata_ref[key]['SLat'],slat)
       rr['ELon'] = max(metadata_ref[key]['ELon'],elon)
       rr['WLon'] = min(metadata_ref[key]['WLon'],wlon)
       rr['Probes'] = ''.join([metadata_ref[key]['Probes'],' ',datafile.ProbeName])
    metadata_ref[key] = rr

    datafile.close()

def get_png_metadata(filename):
    key = filename.split('/')[-1].split('_')[2]
    nlat = metadata_ref[key]["NLat"]
    slat = metadata_ref[key]["SLat"]
    elon = metadata_ref[key]["ELon"]
    wlon = metadata_ref[key]["WLon"]
    start = metadata_ref[key]["start"]
    end = metadata_ref[key]["end"]

    key0 = os.path.basename(filename)
    metadata[key0] = {}
    metadata[key0]["temporal"] = [x.strftime('%Y-%m-%dT%H:%M:%SZ') for x in [start,end]]
    metadata[key0]["wnes_geometry"] = [str(round(x,3)) for x in [wlon,nlat,elon,slat]]
    metadata[key0]['SizeMBDataGranule'] = str(round(1E-6 * os.path.getsize(filename), 2))
    with open(filename, 'rb') as file:
        metadata[key0]['checksum'] = md5(file.read()).hexdigest()
    metadata[key0]['format'] = 'PNG'


for root, dirs, files in os.walk(data_dir):
    dirs.sort()
    for filename in sorted(files):
        if filename.endswith('.nc'):
           #print(os.path.join(root, filename))
           get_nc_metadata(os.path.join(root, filename))

for root, dirs, files in os.walk(data_dir):
    dirs.sort()
    for filename in sorted(files):
        if filename.endswith('.tar'):
           #print(os.path.join(root, filename))
           get_png_metadata(os.path.join(root, filename))

with open('parprbimpactsRefData.json', 'w') as fp:
     json.dump(metadata, fp)
