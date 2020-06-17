import os                #-- Needed for locating python metadata codebase
from datetime import datetime, timedelta
import json

metadata = {}

def read_DC8NAV_metadata(filename, fp):
    r = {}
    r1 = {}
    maxlat = -90.0
    minlat = 90.0
    maxlon = -180.0
    minlon = 180.0
    stTime = datetime(2100, 1, 1)
    endTime = datetime(1900, 1, 1)
    k1 = 'default'
    r1['start'] = stTime.strftime('%Y-%m-%dT%H:%M:%SZ')
    r1['end'] = endTime.strftime('%Y-%m-%dT%H:%M:%SZ')
    r1['NLat'] = maxlat
    r1['SLat'] = minlat
    r1['ELon'] = maxlon
    r1['WLon'] = minlon
    metadata[k1] = r1

#-- get doy from filename
    fname = filename.split('/')[-1]
    ydoy_str = fname.split('.')[0]
    doy_from_filename = int(ydoy_str[2:5])

    for line in fp:
        if line.find('C') >= 0:
            tokens = line.split()
            doy = int(tokens[1])
            if doy != doy_from_filename and doy != (doy_from_filename + 1):
                continue
            if filename.find('98238') >= 0:
                if doy != 238:
                    print(line)
#-- CAMEX-3 campaign 1998
            time_str = '19980101T' + tokens[2]
            dt = datetime.strptime(time_str, "%Y%m%dT%H:%M:%S.%f") + timedelta(days=doy-1)
            lat = float(tokens[3])
            if lat >= 0:
                lat = lat + float(tokens[4])/60.0
            else:
                lat = lat - float(tokens[4])/60.0
            lon = float(tokens[5])
            if lon >= 0:
                lon = lon + float(tokens[6])/60.0
            else:
                lon = lon - float(tokens[6])/60.0

            if lat > maxlat:
                maxlat = lat
            if lat < minlat:
                minlat = lat
            if lon > maxlon:
                maxlon = lon
            if lon < minlon:
                minlon = lon
            if dt < stTime:
                stTime = dt
            if dt > endTime:
                endTime = dt

    fp.close()

#-- keep daily metadata for browse images
    key = ydoy_str
    rr = {}

    r['start'] = stTime
    r['end'] = endTime
    r['NLat'] = maxlat
    r['SLat'] = minlat
    r['ELon'] = maxlon
    r['WLon'] = minlon

    rr['start'] = r['start'].strftime('%Y-%m-%dT%H:%M:%SZ')
    rr['end'] = r['end'].strftime('%Y-%m-%dT%H:%M:%SZ')
    rr['NLat'] = r['NLat']
    rr['SLat'] = r['SLat']
    rr['ELon'] = r['ELon']
    rr['WLon'] = r['WLon']
    metadata[key] = rr

    return r


#-- obtain flight metadata from DC8DADS dataset
navdir = "/Users/navaneeth/dc8lase/dc8/"
for subdir, dirs, files in os.walk(navdir):
    for cfile in files:
        fullfilename = os.path.join(subdir, cfile)
        if fullfilename.find('_flt.txt') >= 0:
            fp = open(fullfilename, 'r')
            read_DC8NAV_metadata(fullfilename, fp)

print(metadata)
print(len(metadata))
with open('../dc8laseRefData.json', 'w') as fp:
    json.dump(metadata, fp)
