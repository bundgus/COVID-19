import glob
import pandas as pd
import numpy as np
import csv
import jinja2


def combine_csv(source_files, output_file):
    keep_header_row = True  # only keep the first header row from the first file to avoid header duplication
    with open(output_file, "w") as outfile:
        for f in source_files:
            fn = f.split('\\')[1]
            date_time = f"{fn[6:10]}-{fn[0:2]}-{fn[3:5]}T00:00:00Z"
            with open(f, "r") as infile:
                if keep_header_row:
                    line = infile.readline().strip()
                    print(f'{line},date_time', file=outfile)
                    keep_header_row = False
                else:
                    infile.readline()  # throw away the first header line for subsequent files
                    # print(file=outfile, end='')
                for line in infile:
                    if line.strip() != '':
                        print(f'{line.strip()},{date_time}', file=outfile, end='\n')


directories = [
    {'read_files': glob.glob("csse_covid_19_data/csse_covid_19_daily_reports/daily_format_1/*.csv"),
     'summary_file': "csse_covid_19_data/daily_format_1.csv"
     },
    {'read_files': glob.glob("csse_covid_19_data/csse_covid_19_daily_reports/daily_format_2/*.csv"),
     'summary_file': "csse_covid_19_data/daily_format_2.csv"
     },
    {'read_files': glob.glob("csse_covid_19_data/csse_covid_19_daily_reports/daily_format_3/*.csv"),
     'summary_file': "csse_covid_19_data/daily_format_3.csv"
     }
]

for directory in directories:
    combine_csv(directory['read_files'], directory['summary_file'])

df1 = pd.read_csv('csse_covid_19_data/daily_format_1.csv')
df1 = df1[['Province/State', 'Country/Region', 'Last Update', 'Confirmed',
           'Deaths', 'Recovered', 'date_time']]
df1.columns = ['Province_State', 'Country_Region', 'Last_Update', 'Confirmed',
               'Deaths', 'Recovered', 'date_time']
df1['FIPS'] = np.NaN
df1['Admin2'] = ''
df1['Lat'] = np.NaN
df1['Long_'] = np.NaN
df1['Active'] = np.NaN
df1['Combined_Key'] = ''
df1['Country_Region'] = df1['Country_Region'].str.strip()

df2 = pd.read_csv('csse_covid_19_data/daily_format_2.csv')
df2 = df2[['Province/State', 'Country/Region', 'Last Update', 'Confirmed',
           'Deaths', 'Recovered', 'Latitude', 'Longitude', 'date_time']]
df2.columns = ['Province_State', 'Country_Region', 'Last_Update', 'Confirmed',
               'Deaths', 'Recovered', 'Lat', 'Long_', 'date_time']
df2['FIPS'] = np.NaN
df2['Admin2'] = ''
df2['Active'] = np.NaN
df2['Combined_Key'] = ''
df2['Country_Region'] = df2['Country_Region'].str.strip()

df3 = pd.read_csv('csse_covid_19_data/daily_format_3.csv')
df3 = df3[['FIPS', 'Admin2', 'Province_State', 'Country_Region', 'Last_Update',
           'Lat', 'Long_', 'Confirmed', 'Deaths', 'Recovered', 'Active',
           'Combined_Key', 'date_time']]

df3['Country_Region'] = df3['Country_Region'].str.strip()

frames = [df1, df2, df3]
df = pd.concat(frames, sort=True)
# df.to_csv('daily_combined.csv', index=False)

# df = pd.read_csv('daily_combined.csv')
df = df[['Active', 'Admin2', 'Combined_Key', 'Confirmed', 'Country_Region',
         'Deaths', 'FIPS', 'Last_Update', 'Province_State',
         'Recovered', 'date_time']]
cc_map_df = pd.read_csv('reference_data/country_code_lat_lon.csv')
merged_df = df.merge(cc_map_df, left_on='Country_Region', right_on='covid_country', how='right')
merged_df = merged_df[['Active', 'Confirmed',
                       'Deaths', 'Recovered',
                       'date_time', 'iso_country', 'country_code', 'lat',
                       'long_']]
sum_grouped_df = merged_df.groupby(['date_time', 'iso_country', 'country_code', 'lat',
                                    'long_']).sum().reset_index()
sum_grouped_df.columns = ['date_time', 'country_name', 'iso_a2', 'lat', 'lon', 'active',
                          'confirmed', 'deaths', 'recovered']

# fill missing time series values

unique_date_time = sum_grouped_df['date_time'].unique()

max_active = sum_grouped_df['active'].max()
min_active = sum_grouped_df['active'].min()
max_confirmed = sum_grouped_df['confirmed'].max()
min_confirmed = sum_grouped_df['confirmed'].min()
max_deaths = sum_grouped_df['deaths'].max()
min_deaths = sum_grouped_df['deaths'].min()
max_recovered = sum_grouped_df['recovered'].max()
min_recovered = sum_grouped_df['recovered'].min()

new_rows = []
for ud in unique_date_time:
    new_rows.append({'date_time': ud,
                     'iso_a2': 'max',
                     'country_name': 'max',
                     'active': max_active,
                     'confirmed': max_confirmed,
                     'deaths': max_deaths,
                     'recovered': max_recovered,
                     'lat': 90,
                     'lon': 90
                     })
    new_rows.append({'date_time': ud,
                     'iso_a2': 'min',
                     'country_name': 'min',
                     'active': min_active,
                     'confirmed': min_confirmed,
                     'deaths': min_deaths,
                     'recovered': min_recovered,
                     'lat': 90,
                     'lon': 90
                     })

min_max_df = sum_grouped_df.append(new_rows, ignore_index=True)

min_max_df['date_time'] = pd.to_datetime(min_max_df['date_time'])
r = pd.date_range(start=min_max_df['date_time'].min(),
                  end=min_max_df['date_time'].max())

grouped = min_max_df.groupby('iso_a2', as_index=False)

individual_group_dataframes = [pd.DataFrame(y) for x, y in grouped]

filled_df = pd.DataFrame()

for idf in individual_group_dataframes:
    country_name = idf['country_name'].iloc[0]
    iso_a2 = idf['iso_a2'].iloc[0]
    lat = idf['lat'].iloc[0]
    lon = idf['lon'].iloc[0]
    tdf = idf.set_index('date_time').reindex(r)
    tdf['country_name'] = country_name
    tdf['iso_a2'] = iso_a2
    tdf['lat'] = lat
    tdf['lon'] = lon
    tdf = tdf.ffill(axis='rows').fillna(0.0).rename_axis('date_time').reset_index()
    tdf['date_time'] = tdf['date_time'].astype(str)
    filled_df = filled_df.append(tdf)

# join with geojson geometry
# df_geojson = pd.read_csv('geojson_assets/countries_simplified_geojson.csv')
# final_df = filled_df.merge(df_geojson, left_on='iso_a2', right_on='iso_a2', how='left')

filled_df['active'] = filled_df['confirmed'] - filled_df['deaths'] - filled_df['recovered']
filled_df['mortality_rate'] = (filled_df['deaths'] / filled_df['confirmed']).fillna(0.0)
final_df = filled_df.sort_values(by=['date_time'])

final_df.to_csv('docs/daily_by_country.csv', index=False, quoting=csv.QUOTE_NONNUMERIC)

templateLoader = jinja2.FileSystemLoader(searchpath="./")
templateEnv = jinja2.Environment(loader=templateLoader)
TEMPLATE_FILE = "kepler_report_template_country_by_date.html.j2"
template = templateEnv.get_template(TEMPLATE_FILE)
# json_data = final_df[final_df['date_time'] >= '2020-03-01 00:00:00+00:00'].to_json(orient='values')
json_data = final_df.to_json(orient='values')
outputText = template.render(data=json_data)  # this is where to put args to the template renderer
with open('docs/covid-19-country-timeline.html', 'w') as html_report_file:
    html_report_file.write(outputText)
