#%%
import io
import datetime

import pandas
import altair

#%%
#df = pandas.read_csv('../data/2021-05-18T16_29_53-116060678.csv')
df = pandas.read_csv('../data/2021-05-18T16_29_53-116060678.csv')
df = df[df['adm1']=='UK-NIR']

#%%
lineage_lookup = pandas.DataFrame([
    {'WHO label': 'Alpha', 'Pango lineage': 'B.1.1.7'},
    {'WHO label': 'Beta', 'Pango lineage': 'B.1.351'},
    {'WHO label': 'Gamma', 'Pango lineage': 'P.1'},
    {'WHO label': 'Delta', 'Pango lineage': 'B.1.617.2'},
    {'WHO label': 'Epsilon', 'Pango lineage': 'B.1.427'},
    {'WHO label': 'Zeta', 'Pango lineage': 'P.2'},
    {'WHO label': 'Eta', 'Pango lineage': 'B.1.525'},
    {'WHO label': 'Theta', 'Pango lineage': 'P.3'},
    {'WHO label': 'Iota', 'Pango lineage': 'B.1.526'},
    {'WHO label': 'Kappa', 'Pango lineage': 'B.1.617.1'},
    {'WHO label': 'Lambda', 'Pango lineage': 'C.37'},
])

#%%
df['Sample Date'] = pandas.to_datetime(df['sample_date'])
df['Week of sample'] = df['Sample Date'] - pandas.to_timedelta(df['Sample Date'].dt.dayofweek, unit='d')
df = df.merge(lineage_lookup, how='left', left_on='lineage', right_on='Pango lineage')
df['WHO label'] = df['WHO label'].fillna('Other')
lin_by_day = df.groupby(['Sample Date','WHO label']).size().reset_index(name='count')
lin_by_week = df.groupby(['Week of sample','WHO label']).size().rename('count')
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
p = altair.vconcat(
    altair.Chart(
        lin_by_week[lin_by_week['Week of sample']>lin_by_week['Week of sample'].max()-pandas.to_timedelta(84, unit='d')]
    ).mark_line().encode(
        x = altair.X('Week of sample:T', axis=altair.Axis(title='', labels=False, ticks=False)),
        y = altair.Y('count:Q', axis=altair.Axis(title='Samples')),
        color='WHO label'
    ).properties(
        height=225,
        width=800,
        title='NI COVID-19 variants identified by COG-UK over the most recent 12 weeks'
    ),
    altair.Chart(
        lin_pc_by_week[lin_pc_by_week['Week of sample']>lin_pc_by_week['Week of sample'].max()-pandas.to_timedelta(84, unit='d')]
    ).mark_area().encode(
        x = 'Week of sample:T',
        y = altair.Y('sum(count):Q', axis=altair.Axis(format='%', title='% of samples')),
        color='WHO label'
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
p.save('ni-variants-%s.png'%datetime.datetime.now().date().strftime('%Y-%d-%m'))
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
