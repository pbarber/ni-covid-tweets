# %%
import datetime

import pandas
from plot_shared import plot_points_average_and_trend

# %%
df = pandas.read_csv('https://api.coronavirus.data.gov.uk/v2/data?areaType=nation&metric=cumPeopleVaccinatedFirstDoseByPublishDate&metric=cumPeopleVaccinatedThirdInjectionByPublishDate&metric=cumPeopleVaccinatedSecondDoseByPublishDate&format=csv')
df.rename(columns={
    'cumPeopleVaccinatedFirstDoseByPublishDate': 'First',
    'cumPeopleVaccinatedSecondDoseByPublishDate': 'Second',
    'cumPeopleVaccinatedThirdInjectionByPublishDate': 'Third/Booster',
    'areaName': 'Nation',
    'date': 'Publication Date'
}, inplace=True)
df = df.drop(columns=['areaCode','areaType']).melt(id_vars=['Publication Date','Nation'], var_name='Dose', value_name='People')
all = df[df['Nation']=='Northern Ireland'][['Publication Date','Dose','People']]
#ni.rename(columns={'Date':'Publication Date','Total':'People'}, inplace=True)
#all = ni
#all = history.merge(ni, on=['Publication Date','Dose'], how='outer', suffixes=('','_bot'))
#all['People'] = all['People'].fillna(all['People_bot'])
#all = all[['Publication Date','Dose','People']]

# %%
boosters = all[all['Dose']=='Third/Booster'][['Publication Date','People']]
boosters['Publication Date'] = pandas.to_datetime(boosters['Publication Date'])
dose2s = all[all['Dose']=='Second'][['Publication Date','People']]
dose2s['Publication Date'] = pandas.to_datetime(dose2s['Publication Date'])
dose2s['Booster Target Date 6M'] = pandas.to_datetime(dose2s['Publication Date']) + pandas.to_timedelta(183, unit='d')
dose2s['Booster Target Date 7M'] = pandas.to_datetime(dose2s['Publication Date']) + pandas.to_timedelta(183+31, unit='d')
dose2s['Booster Target Date 8M'] = pandas.to_datetime(dose2s['Publication Date']) + pandas.to_timedelta(183+62, unit='d')
boosters = boosters.merge(dose2s[['Booster Target Date 6M','People']], left_on='Publication Date', right_on='Booster Target Date 6M', how='left', suffixes=('', '_target'))
boosters = boosters[['Publication Date','People','People_target']]
boosters.rename(columns={'People':'Third/Booster doses', 'People_target': 'Second doses 6 months earlier'}, inplace=True)
boosters = boosters.merge(dose2s[['Booster Target Date 7M','People']], left_on='Publication Date', right_on='Booster Target Date 7M', how='left', suffixes=('', '_target'))
boosters = boosters[['Publication Date','Third/Booster doses','Second doses 6 months earlier','People']]
boosters.rename(columns={'People': 'Second doses 7 months earlier'}, inplace=True)
boosters = boosters.merge(dose2s[['Booster Target Date 8M','People']], left_on='Publication Date', right_on='Booster Target Date 8M', how='left', suffixes=('', '_target'))
boosters = boosters[['Publication Date','Third/Booster doses','Second doses 6 months earlier','Second doses 7 months earlier','People']]
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
        'Data from https://coronavirus.data.gov.uk',
        'https://twitter.com/ni_covid19_data'
    ]
)

# %%
p = plot_points_average_and_trend(
    [
        {
            'points': None,
            'line': boosters[boosters['Publication Date'] > '2021-10-01'].set_index(['Publication Date','Metric'])['Doses'],
            'colour': 'Metric',
            'date_col': 'Publication Date',
            'x_title': 'Date',
            'y_title': 'Total doses',
            'scales': ['linear'],
            'height': 450,
            'width': 800,
        },
    ],
    'NI COVID-19 booster/third vaccination progress vs second dose up to %s' %(
        datetime.datetime.today().strftime('%A %-d %B %Y'),
    ),
    [
        'Data from https://coronavirus.data.gov.uk',
        'https://twitter.com/ni_covid19_data'
    ]
)
p.save('ni-boosters-%s.png'%(datetime.datetime.now().date().strftime('%Y-%m-%d')))
p
# %%
