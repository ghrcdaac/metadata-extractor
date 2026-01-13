import json
with open('lookup.json','r') as fp:
     md = json.load(fp)

for key in md.keys():
       west = float(md[key]['west'])
       east = float(md[key]['east'])
       north = float(md[key]['north'])
       south = float(md[key]['south'])
       if west < -80. or east > -75: # or north > 45 or south < 35:
          print(key)
          print(md[key])


