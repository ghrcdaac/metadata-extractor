from zipfile import ZipFile
import json
import os
import pathlib

#summary metadata
#"north": 57.267, "south": -57.312, "east": 180.0, "west": -180.0
#north, south, east, west = [57.267, -57.312, 180.0, -180.0]

file_excluded = ['OR_GLM-L2-CIERRA-DB_GOES-EAST_s20192931845000.nc',
                 'OR_GLM-L2-CIERRA-DB_GOES-EAST_s20193132345000.nc',
                 'OR_GLM-L2-CIERRA-DB_GOES-WEST_s20203590215000.nc',
                 'OR_GLM-L2-CIERRA-DB_GOES-WEST_s20203591600000.nc',
                 'OR_GLM-L2-CIERRA-DB_GOES-WEST_s20210122000000.nc']



#Get netCDF-4 metadata attributes from lookup zip
lookup_zip_path = os.path.join(pathlib.Path(__file__).parent.absolute(),
                      f"../glmcierra.zip")
lookup_zip_path_orig = os.path.join(pathlib.Path(__file__).parent.absolute(),
                      f"../glmcierra_orig.zip")
os.rename(lookup_zip_path, lookup_zip_path_orig)

with ZipFile(lookup_zip_path_orig) as lookup_zip_orig:
     with lookup_zip_orig.open("lookup.json") as collection_lookup_orig:
          metadata = json.load(collection_lookup_orig)
with ZipFile(lookup_zip_path_orig) as lookup_zip_orig:
     with lookup_zip_orig.open("summary.json") as summary_meta_orig:
          summary_meta = json.load(summary_meta_orig)

#{"OR_GLM-L2-CIERRA-DB_GOES-EAST_s20170122300000.nc": {"start": "2017-01-12T23:00:00Z", "end": "2017-01-12T23:14:59Z", "north": "56.429", "south": "-56.141", "east": "-33.987", "west": "-148.712", "format": "netCDF-4", "sizeMB": 0.92}

for key in file_excluded:
    metadata[key]["north"] = "57.267"
    metadata[key]["south"] = "-57.312"
    metadata[key]["east"] = "180.0"
    metadata[key]["west"] = "-180.0" 

with open('./lookup.json', 'w') as fp:
    json.dump(metadata, fp)
with open('./summary.json', 'w') as fp:
    json.dump(summary_meta, fp)

# The below 2 line can also be substituted by the command line "zip glmcierra.zip lookup.json"
with ZipFile('../glmcierra.zip', 'w') as myzip:
    myzip.write('lookup.json')
    myzip.write('summary.json')
