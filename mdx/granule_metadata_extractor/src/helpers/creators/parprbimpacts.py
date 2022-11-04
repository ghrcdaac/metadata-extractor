import json
import os
import re
import shutil
import sys
import tarfile
from datetime import datetime, timedelta
from hashlib import md5
import numpy as np
from netCDF4 import Dataset
#from spectral.io import envi

file_dir = "/ftp/ops/public/pub/fieldCampaigns/impacts/NCAR_Particle_Probes/data"

metadata = {}

def get_metadata(filename):
    datafile = Dataset(filename)
    lats = np.array(datafile['LAT'][:])
    lons = np.array(datafile['LON'][:])
    sec = np.array(datafile['time'][:])
    ref_time = datetime.strptime(datafile.FlightDate,'%m/%d/%Y')

#------------------------------------------------------------------
    key = filename.split('/')[-1].split('_')[2] #i.e., 20200108

    rr={}
    if key not in metadata.keys():
       rr['start'], rr['end'] = [ref_time+timedelta(seconds=sec.min()),
                                 ref_time+timedelta(seconds=sec.max())]
       rr['NLat'], rr['SLat'], rr['ELon'], rr['WLon'] = [float(np.nanmax(lats)),
                                                         float(np.nanmin(lats)),
                                                         float(np.nanmax(lons)),
                                                         float(np.nanmin(lons))]
    else:
       #If metadata[key] already exists,update values
       rr['start'] = min(metadata[key]['start'],ref_time+timedelta(seconds=sec.min()))
       rr['end'] = max(metadata[key]['end'],ref_time+timedelta(seconds=sec.max()))
       rr['NLat'] = max(metadata[key]['NLat'],np.nanmax(lats))
       rr['SLat'] = min(metadata[key]['SLat'],np.nanmin(lats))
       rr['ELon'] = max(metadata[key]['ELon'],np.nanmax(lons))
       rr['WLon'] = min(metadata[key]['WLon'],np.nanmin(lons))
    metadata[key] = rr
    datafile.close()


if __name__ == "__main__":
    for root, dirs, files in os.walk(file_dir):
        dirs.sort()
        for filename in sorted(files):
            if filename.endswith('.nc'):
               #print(os.path.join(root, filename))
               get_metadata(os.path.join(root, filename))

    for key in metadata.keys():
        metadata[key]['start'] = metadata[key]['start'].strftime('%Y-%m-%dT%H:%M:%SZ')
        metadata[key]['end'] = metadata[key]['end'].strftime('%Y-%m-%dT%H:%M:%SZ')
        metadata[key]['NLat'] = float(metadata[key]['NLat'])
        metadata[key]['SLat'] = float(metadata[key]['SLat'])
        metadata[key]['ELon'] = float(metadata[key]['ELon'])
        metadata[key]['WLon'] = float(metadata[key]['WLon'])

    with open('parprbimpactsRefData.json', 'w') as fp:
        json.dump(metadata, fp)
