#%%
import pandas
import altair

#%%
df = pandas.read_csv('../data/2021-05-18T16_29_53-116060678.csv')
df = df[df['adm1']=='UK-NIR']

#%%
lin_by_day = df.groupby(['sample_date','lineage']).size().reset_index(name='count')
lin_by_week = df.groupby(['epi_week','lineage']).size().reset_index(name='count')
lineage = df.groupby('lineage').size().reset_index(name='count')

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