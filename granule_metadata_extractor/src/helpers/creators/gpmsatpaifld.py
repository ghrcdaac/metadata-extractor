import os, json
from datetime import datetime, timedelta
import xml.etree.ElementTree as ET

metadata = {}


def read_KML_metadata(xmlfile):
    fname = xmlfile.split('/')[-1]
    fname = fname.split('.')[0]
    dstr = fname.split('_')[-1]

    stTime = datetime(2100, 1, 1)
    endTime = datetime(1900, 1, 1)
    maxlat = -90.0
    minlat = 90.0
    maxlon = -180.0
    minlon = 180.0

    # create element tree object
    tree = ET.parse(xmlfile)

    # get root element
    root = tree.getroot()

    # -- from coordinates element get bounding box
    for pp in root.iter('{http://earth.google.com/kml/2.0}coordinates'):
        tkn = pp.text.split(',')
        if len(tkn) == 2:
            lon = float(tkn[0])
            lat = float(tkn[1])
            if lon > maxlon:
                maxlon = lon
            if lon < minlon:
                minlon = lon
            if lat > maxlat:
                maxlat = lat
            if lat < minlat:
                minlat = lat

    for qq in root.iter('{http://earth.google.com/kml/2.0}description'):
        tstr = qq.text.split()[-2]
        dtstr = dstr + 'T' + tstr
        dt = datetime.strptime(dtstr, '%Y%m%dT%H:%M:%S')
        if dt > endTime:
            endTime = dt
        if dt < stTime:
            stTime = dt

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
    metadata[dstr] = rr
    print(metadata)
    print(r)
    return r





navdir = "/Users/navaneeth/ghrc_projects/valid/"


for subdir, dirs, files in os.walk(navdir):
    for cfile in files:
        fullfilename = os.path.join(subdir, cfile)
        if fullfilename.find('.kml') >= 0:
            fp = open(fullfilename, 'r')
            read_KML_metadata(fullfilename)

print(metadata)
print(len(metadata))
with open('../gpmsatpaifldRefData.json', 'w') as fp:
    json.dump(metadata, fp)