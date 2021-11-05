#%%
import io
import datetime

import pandas
import altair
import requests

#%%
# Dataframe for converting between pango lineage and WHO labels
# Get the mapping from the raw Github URL
resp = requests.get('https://github.com/pbarber/covid19-pango-lineage-to-who-label/raw/main/mapping.json')
# Make sure that the request was successful
resp.raise_for_status()
# Convert the request data to a Python dictionary
mapping = resp.json()
# Expand the Pango column
mapping = pandas.DataFrame(mapping).explode('Pango lineages').reset_index(drop=True)

def match(lineage, col):
    return (col.str.slice(stop=len(lineage))==lineage)

#%%
df = pandas.read_csv('../data/2021-10-18T11_01_40-95249580.csv')
df = df[df['adm1']=='UK-NIR']
df['Sample Date'] = pandas.to_datetime(df['sample_date'])
df['Week of sample'] = df['Sample Date'] - pandas.to_timedelta(df['Sample Date'].dt.dayofweek, unit='d')
# Join the lineage data
matches = mapping['Pango lineages'].apply(match, col=df['lineage'])
match_idx = matches.idxmax()
# Filter out indexes where there is no match
match_idx[match_idx==matches.idxmin()] = pandas.NA
df['idx'] = match_idx
# Join to the mapping based on indexes
df = df.merge(mapping, how='left', left_on='idx', right_index=True).drop(columns=['idx','Pango lineages'])
df['WHO label'] = df['WHO label'].fillna('Other')
lin_by_day = df.groupby(['Sample Date','WHO label']).size().reset_index(name='count')
lin_by_week = df.groupby(['Week of sample','lineage']).size().rename('count')
lin_pc_by_week = lin_by_week/lin_by_week.groupby(level=0).sum()
lin_by_week = pandas.DataFrame(lin_by_week).reset_index()
lin_pc_by_week = pandas.DataFrame(lin_pc_by_week).reset_index()
lineage = df.groupby('WHO label').size().reset_index(name='count')
lin_by_area = df.groupby(['iso_3166_code','WHO label']).size().reset_index(name='count')

#%%
altair.Chart(
    lin_by_day[lin_by_day['Sample Date'] > df['Sample Date'].max()-pandas.to_timedelta(84, unit='d')]
).mark_area().encode(
    x = 'Sample Date:T',
    y = 'count:Q',
    color='WHO label'
)

#%%
toplot = lin_by_week[(lin_by_week['Week of sample']>lin_by_week['Week of sample'].max()-pandas.to_timedelta(84, unit='d')) & (lin_by_week[lin_by_week['Week of sample']<lin_by_week['Week of sample'].max()-pandas.to_timedelta(21, unit='d'))]
p = altair.vconcat(
    altair.Chart(
        toplot
    ).mark_line().encode(
        x = altair.X('Week of sample:T', axis=altair.Axis(title='', labels=False, ticks=False)),
        y = altair.Y('count:Q', axis=altair.Axis(title='Samples')),
        color='lineage'
    ).properties(
        height=225,
        width=800,
        title='NI COVID-19 variants identified by COG-UK over the most recent 12 weeks'
    ),
    altair.Chart(
        toplot
    ).mark_area().encode(
        x = 'Week of sample:T',
        y = altair.Y('sum(count):Q', axis=altair.Axis(format='%', title='% of samples')),
        color='lineage'
    ).properties(
        height=225,
        width=800,
    )
).properties(
    title=altair.TitleParams(
        ['Variant identification can take up to 3 weeks, so recent totals are likely to be revised upwards',
        'https://twitter.com/ni_covid19_data on %s'  %datetime.datetime.now().date().strftime('%A %-d %B %Y')],
        baseline='bottom',
        orient='bottom',
        anchor='end',
        fontWeight='normal',
        fontSize=10,
        dy=10
    ),
)
p.save('ni-variants-lineage-%s.png'%datetime.datetime.now().date().strftime('%Y-%d-%m'))
p

#%%
lin_by_area[lin_by_area['WHO label']=='Delta']
# %%
lineage[lineage['WHO label'].str.strip()=='Delta']
# %%
len(df)
# %%
df[df['sequence_name'].isin(['COG368878','COG934551','COG518549','COG743441','COG680501','COG754556','COG477927','COG755039','COG606825','COG900779'])]
# %%
resp = requests.get('http://sars2.cvr.gla.ac.uk/cog-uk/session/8508cd8c20a80bd6255156d95e5626e3/download/downloadTable3?w=')
resp.raise_for_status()
stream = io.BytesIO(resp.content)
df = pandas.read_csv(stream)
# %%
df[['Variant','Northern Ireland','Northern Ireland 28 Days']]
# %%
