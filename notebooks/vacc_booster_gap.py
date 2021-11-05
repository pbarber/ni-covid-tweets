# %%
import datetime

import pandas
import altair
from plot_shared import plot_points_average_and_trend

# %%
df = pandas.read_csv('https://api.coronavirus.data.gov.uk/v2/data?areaType=nation&metric=cumPeopleVaccinatedFirstDoseByPublishDate&metric=cumPeopleVaccinatedSecondDoseByPublishDate&format=csv')
df.rename(columns={
    'cumPeopleVaccinatedFirstDoseByPublishDate': 'First',
    'cumPeopleVaccinatedSecondDoseByPublishDate': 'Second',
    'areaName': 'Nation',
    'date': 'Publication Date'
}, inplace=True)
df = df.drop(columns=['areaCode','areaType']).melt(id_vars=['Publication Date','Nation'], var_name='Dose', value_name='People')

# %%
ni = pandas.read_csv('../sam/doses.csv')
ni['Dose'] = ni['Dose'].str.replace('Dose 1', 'First')
ni['Dose'] = ni['Dose'].str.replace('Dose 2', 'Second')
ni['Dose'] = ni['Dose'].str.replace('Dose 3', 'Third')

# %%
history = df[df['Nation']=='Northern Ireland'][['Publication Date','Dose','People']]
ni.rename(columns={'Date':'Publication Date','Total':'People'}, inplace=True)
all = history.merge(ni, on=['Publication Date','Dose'], how='outer', suffixes=('','_bot'))
all['People'] = all['People'].fillna(all['People_bot'])
all = all[['Publication Date','Dose','People']]

# %%
boosters = all[all['Dose']=='Booster'][['Publication Date','People']]
boosters['Publication Date'] = pandas.to_datetime(boosters['Publication Date'])
dose2s = all[all['Dose']=='Second'][['Publication Date','People']]
dose2s['Publication Date'] = pandas.to_datetime(dose2s['Publication Date'])
dose2s['Booster Target Date 6M'] = pandas.to_datetime(dose2s['Publication Date']) + pandas.to_timedelta(183, unit='d')
dose2s['Booster Target Date 7M'] = pandas.to_datetime(dose2s['Publication Date']) + pandas.to_timedelta(183+31, unit='d')
dose2s['Booster Target Date 8M'] = pandas.to_datetime(dose2s['Publication Date']) + pandas.to_timedelta(183+62, unit='d')
boosters = boosters.merge(dose2s[['Booster Target Date 6M','People']], left_on='Publication Date', right_on='Booster Target Date 6M', how='left', suffixes=('', '_target'))
boosters = boosters[['Publication Date','People','People_target']]
boosters.rename(columns={'People':'Booster doses', 'People_target': 'Second doses 6 months earlier'}, inplace=True)
boosters = boosters.merge(dose2s[['Booster Target Date 7M','People']], left_on='Publication Date', right_on='Booster Target Date 7M', how='left', suffixes=('', '_target'))
boosters = boosters[['Publication Date','Booster doses','Second doses 6 months earlier','People']]
boosters.rename(columns={'People': 'Second doses 7 months earlier'}, inplace=True)
boosters = boosters.merge(dose2s[['Booster Target Date 8M','People']], left_on='Publication Date', right_on='Booster Target Date 8M', how='left', suffixes=('', '_target'))
boosters = boosters[['Publication Date','Booster doses','Second doses 6 months earlier','Second doses 7 months earlier','People']]
boosters.rename(columns={'People': 'Second doses 8 months earlier'}, inplace=True)
boosters = boosters.melt(id_vars='Publication Date', var_name='Metric', value_name='Doses')

# %%
plot_points_average_and_trend(
    [
        {
            'points': None,
            'line': all.set_index(['Publication Date','Dose'])['People'],
            'colour': 'Dose',
            'date_col': 'Publication Date',
            'x_title': 'Publication Date',
            'y_title': 'Total doses',
            'scales': ['linear'],
            'height': 450,
            'width': 800,
        },
    ],
    'NI COVID-19 vaccination progress up to %s' %(
        datetime.datetime.today().strftime('%A %-d %B %Y'),
    ),
    [
        'Dose 1/2 data from PHE dashboard/API, Dose 3/Booster collected from NI dashboard',
        'https://twitter.com/ni_covid19_data'
    ]
)

# %%
p = plot_points_average_and_trend(
    [
        {
            'points': None,
            'line': boosters[boosters['Metric'] != 'Second doses 6 months earlier'].set_index(['Publication Date','Metric'])['Doses'],
            'colour': 'Metric',
            'date_col': 'Publication Date',
            'x_title': 'Date',
            'y_title': 'Total doses',
            'scales': ['linear'],
            'height': 450,
            'width': 800,
#            'colour_domain': ['Booster doses','Second doses 6 months earlier','Second doses 7 months earlier','Second doses 8 months earlier'],
#            'colour_range': ['#ff0000','#2b7e9e','#52b4cf','#7eedff'],
        },
    ],
    'NI COVID-19 booster vaccination progress vs second dose up to %s' %(
        datetime.datetime.today().strftime('%A %-d %B %Y'),
    ),
    [
        'Dose 2 data from PHE dashboard/API, Booster data collected from NI dashboard',
        'https://twitter.com/ni_covid19_data'
    ]
)
p.save('ni-boosters-%s.png'%(datetime.datetime.now().date().strftime('%Y-%m-%d')))
p
# %%
