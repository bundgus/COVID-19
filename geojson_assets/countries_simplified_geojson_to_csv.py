import json

with open('countries_simplified_geojson.csv', 'w') as out_file:
    print('iso_a2,_geojson', file=out_file)
    with open('countries_simplified.geojson') as f:
        j = json.loads(f.read())
        f = j['features']
        for jf in f:
            iso_a2 = jf['properties']['ISO_A2']
            json_field = json.dumps(jf).replace('"', '""')
            print(f'{iso_a2},"{json_field}"', file=out_file)
