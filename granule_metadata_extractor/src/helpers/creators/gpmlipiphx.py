import os, json
from datetime import datetime, timedelta
import xml.etree.ElementTree as ET

metadata = {}

def read_LIP_metadata(txtfile):

    fname = txtfile.split('/')[-1]
    fname = fname.split('.')[0]
    key = fname.split('_')[-1]

    maxlat = -90.0
    minlat = 90.0
    maxlon = -180.0
    minlon = 180.0
    stTime = datetime(2100, 1, 1)
    endTime = datetime(1900, 1, 1)

    with open(txtfile,'rb') as f:
        fp = f.readlines() 
    f.close()

    for line in fp:
        tkn = line.split()
        dtstr = tkn[0].decode('utf-8') + 'T' + tkn[1].decode('utf-8')
        dt = datetime.strptime(dtstr, '%Y-%m-%dT%H:%M:%S.%f')
        if dt > endTime:
            endTime = dt
        if dt < stTime:
            stTime = dt
        lat = float(tkn[5])
        lon = float(tkn[6])
        if lat < minlat:
            minlat = lat
        if lat > maxlat:
            maxlat = lat
        if lon < minlon:
            minlon = lon
        if lon > maxlon:
            maxlon = lon

    r = {}
    r['start'] = stTime
    r['end'] = endTime
    r['NLat'] = maxlat
    r['SLat'] = minlat
    r['ELon'] = maxlon
    r['WLon'] = minlon

    rr = {}
    rr['start'] = r['start'].strftime('%Y-%m-%dT%H:%M:%SZ')
    rr['end'] = r['end'].strftime('%Y-%m-%dT%H:%M:%SZ')
    rr['NLat'] = r['NLat']
    rr['SLat'] = r['SLat']
    rr['ELon'] = r['ELon']
    rr['WLon'] = r['WLon']
    metadata[key] = rr
    print(metadata)
    print(r)
    return r


datadir = "/ftp/ops/public/pub/fieldCampaigns/gpmValidation/iphex/LIP/data/"

for subdir, dirs, files in os.walk(datadir):
    for cfile in files:
        fullfilename = os.path.join(subdir, cfile)
        if fullfilename.find('.txt') >= 0:
            fp = open(fullfilename, 'r')
            read_LIP_metadata(fullfilename)

print(metadata)
print(len(metadata))
with open('../gpmlipiphxRefData.json', 'w') as fp:
    json.dump(metadata, fp)
