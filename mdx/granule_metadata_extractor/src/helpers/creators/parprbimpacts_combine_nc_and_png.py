from zipfile import ZipFile
import json
import os
import pathlib

#Get netCDF-4 metadata attributes from lookup zip
lookup_zip_path = os.path.join(pathlib.Path(__file__).parent.absolute(),
                      f"../parprbimpacts_nc.zip")
with ZipFile(lookup_zip_path) as lookup_zip:
     with lookup_zip.open("lookup.json") as collection_lookup:
          metadata_nc = json.load(collection_lookup)

#Get PNG metadata attributes from lookup zip
lookup_zip_path = os.path.join(pathlib.Path(__file__).parent.absolute(),
                      f"../parprbimpacts_png.zip")
with ZipFile(lookup_zip_path) as lookup_zip:
     with lookup_zip.open("lookup.json") as collection_lookup:
          metadata_png = json.load(collection_lookup)

metadata = metadata_nc
for key in metadata_png.keys():
    metadata[key] = metadata_png[key]

with open('./lookup.json', 'w') as fp:
    json.dump(metadata, fp)

# The below 2 line can also be substituted by the command line "zip parprbimpacts.zip lookup.json"
with ZipFile('../parprbimpacts.zip', 'w') as myzip:
    myzip.write('lookup.json')
