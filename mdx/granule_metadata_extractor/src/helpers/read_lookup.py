import json
with open('lookup.json','r') as fp:
     md = json.load(fp)

for key in md.keys():
    print(key)
    print(md[key])

