#%%
import io

import pandas
import altair

#%%
#df = pandas.read_csv('../data/2021-05-18T16_29_53-116060678.csv')
df = pandas.read_csv('../data/2021-05-24T10_24_59-55748702.csv')
df = df[df['adm1']=='UK-NIR']

#%%
lin_by_day = df.groupby(['sample_date','lineage']).size().reset_index(name='count')
lin_by_week = df.groupby(['epi_week','lineage']).size().reset_index(name='count')
lineage = df.groupby('lineage').size().reset_index(name='count')
lin_by_area = df.groupby(['iso_3166_code','lineage']).size().reset_index(name='count')

#%%
altair.Chart(
    lin_by_day[lin_by_day['sample_date'] > '2021-03-31']
).mark_area().encode(
    x = 'sample_date:T',
    y = 'count:Q',
    color='lineage'
)

#%%
altair.Chart(
    lin_by_week[lin_by_week['epi_week']> 67]
).mark_area().encode(
    x = 'epi_week:O',
    y = 'count:Q',
    color='lineage'
)

#%%
lin_by_area[lin_by_area['lineage']=='B.1.617.2']
# %%
lineage[lineage['lineage'].str.strip()=='B.1.617.2']
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
