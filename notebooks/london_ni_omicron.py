# %%
import json
import pandas
import altair

altair.data_transformers.disable_max_rows()

# %%
def myloader(x):
    try:
        return json.loads(x)
    except:
        print(x)
        raise

df = pandas.read_csv('~/Downloads/data_2021-Dec-23.csv')
df['json_col'] = df['newCasesBySpecimenDateAgeDemographics'].str.replace('\'','"').str.replace(': None',': "NA"').apply(myloader)
df = df[['areaName','date','json_col']]
df = df.explode('json_col')
df.reset_index(inplace=True)
df = df[['areaName','date']].merge(pandas.json_normalize(df['json_col']), left_index=True, right_index=True)
df['rollingRate'] = pandas.to_numeric(df['rollingRate'],errors='coerce')
london_20s = df[(df['date'] > '2021-11-08') & (df['age'].isin(['20_24','25_29']))].groupby(['date','age','areaName']).sum()
london_20s.reset_index(inplace=True)
london_20s['Age_Band_5yr'] = london_20s['age'].str.replace('20_24','Aged 20 - 24').str.replace('25_29','Aged 25 - 29')
london_20s.rename(columns={'rollingRate': 'Positive per 100k', 'date': 'Date'}, inplace=True)

# %%
altair.Chart(
    df[df['date'] > '2021-11-01']
).mark_line().encode(
    x = 'date:T',
    y = 'rollingRate:Q',
    facet = altair.Facet('age',title=None)
)

# %%
case_band_mapping = pandas.DataFrame({
        'Age_Band_5yr': [
            'Aged 0 - 4', 'Aged 10 - 14', 'Aged 15 - 19', 'Aged 20 - 24',
            'Aged 25 - 29', 'Aged 30 - 34', 'Aged 35 - 39', 'Aged 40 - 44',
            'Aged 45 - 49', 'Aged 5 - 9', 'Aged 50 - 54', 'Aged 55 - 59',
            'Aged 60 - 64', 'Aged 65 - 69', 'Aged 70 - 74', 'Aged 75 - 79',
            'Aged 80 & Over', 'Not Known'
        ],
        'Group': [
            '0 - 19', '0 - 19', '0 - 19', '20+',
            '20+', '20+', '20+', '20+',
            '20+', '0 - 19', '20+', '20+',
            '20+', '20+', '20+', '20+',
            '20+', 'Unknown'
        ],
        'Broad Group': [
            'Other', 'Secondary', 'College/Uni', 'Other',
            'Other', 'Parent', 'Parent', 'Parent',
            'Parent', 'Primary', 'Other', 'Other',
            'Over 60', 'Over 60', 'Over 60', 'Over 60',
            'Over 60', 'Other'
        ],
        '10 Year Group': [
            '0 - 4', '10 - 14', '15 - 19', '20 - 29',
            '20 - 29', '30 - 39', '30 - 39', '40 - 49',
            '40 - 49', '5 - 9', '50 - 59', '50 - 59',
            '60 - 69', '60 - 69', '70+', '70+',
            '70+', 'Not Known'
        ]
    })
cases = pandas.read_csv('agebands.csv')
cases = cases.merge(case_band_mapping, how='left', on='Age_Band_5yr')
pops = get_ni_pop_pyramid()
pops = pops[pops['Year']==2020].groupby(['Age Band']).sum()['Population']
bands = cases.groupby(['Age_Band_5yr','Band Start','Band End'], dropna=False).size().reset_index()[['Age_Band_5yr','Band Start','Band End']]
bands = bands[bands['Age_Band_5yr']!='Not Known']
bands.fillna(90, inplace=True)
bands['Band End'] = bands['Band End'].astype(int)
bands['Band Start'] = bands['Band Start'].astype(int)
bands['Year'] = bands.apply(lambda x: range(x['Band Start'], x['Band End']+1), axis='columns')
bands = bands.explode('Year').reset_index()
bands = bands.merge(pops, how='inner', validate='1:1', right_index=True, left_on='Year')
bands = bands.groupby('Age_Band_5yr').sum()['Population']
cases = cases.merge(bands, how='left', on='Age_Band_5yr')
cases['Positive per 100k'] = (100000 * cases['Positive_Tests']) / cases['Population']
overlay = cases[cases['Date'] == cases['Date'].max()]
overlay['Nearest'] = ((overlay['Positive_Tests']/overlay['Positive_Tests'].max()) * 40).astype(int) * (overlay['Positive_Tests'].max() / 40)
ni_20s = cases[(cases['Date'] > '2021-12-08') & (cases['Age_Band_5yr'].isin(['Aged 20 - 24','Aged 25 - 29']))].groupby(['Date','Age_Band_5yr']).sum()
ni_20s['areaName'] = 'NI'
ni_20s.reset_index(inplace=True)

# %%
all = ni_20s.append(london_20s)

# %%
p = altair.vconcat(
    altair.Chart(
        all
    ).mark_line().encode(
        x = 'Date:T',
        y = 'Positive per 100k:Q',
        facet = altair.Facet('Age_Band_5yr:N',title='Age Band'),
        color = altair.Color('areaName',title=None)
    ).properties(
        height=300,
        width=400,
        title=altair.TitleParams(
            ['Data from https://coronavirus.data.gov.uk and NI DoH dashboard',
            'https://twitter.com/ni_covid19_data on %s' %datetime.datetime.now().strftime('%A %-d %B %Y')
            ],
            baseline='bottom',
            orient='bottom',
            anchor='end',
            fontWeight='normal',
            fontSize=10,
            dy=10
        ),
    )
).properties(
    title=altair.TitleParams(
        'Comparison of recent cases per 100k between London and NI',
        anchor='middle',
    )
)
p.save('ni-vs-london-case-growth-%s.png'%(datetime.datetime.now().date().strftime('%Y-%m-%d')))
p


# %%
