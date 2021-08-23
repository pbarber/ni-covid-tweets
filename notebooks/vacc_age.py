# %%
import pandas
import altair

# %%
def clean_cols(x):
    if x[1].startswith('Unnamed'):
        return x[0].rstrip('0123456789,')
    else:
        return x[0].rstrip('0123456789,') + '_' + x[1]

# %% List of NI age bands, with ordering for plotting
age_bands = pandas.DataFrame([
    {'Order': 0, 'Band': 'Under 18', 'Ages': [i for i in range(18)], 'Eng bands': ['Under 18']},
    {'Order': 1, 'Band': '18-29', 'Ages': [i for i in range(18,30)], 'Eng bands': ['18-24','25-29']},
    {'Order': 2, 'Band': '30-39', 'Ages': [i for i in range(30,40)], 'Eng bands': ['30-34','35-39']},
    {'Order': 3, 'Band': '40-49', 'Ages': [i for i in range(40,50)], 'Eng bands': ['40-44','45-49']},
    {'Order': 4, 'Band': '50-59', 'Ages': [i for i in range(50,60)], 'Eng bands': ['50-54','55-59']},
    {'Order': 5, 'Band': '60-69', 'Ages': [i for i in range(60,70)], 'Eng bands': ['60-64','65-69']},
    {'Order': 6, 'Band': '70-79', 'Ages': [i for i in range(70,80)], 'Eng bands': ['70-74','75-79']},
    {'Order': 7, 'Band': '80+', 'Ages': [i for i in range(80,91)], 'Eng bands': ['80+']},
])
age_bands_ons = age_bands.explode('Ages').reset_index()
age_bands_eng = age_bands.explode('Eng bands').reset_index()

# %% Load the 2020 population data for NI and convert to the NI vaccine reporting bands
ni_pop = pandas.read_csv('ni_ons_pyramid.csv')
ni_pop = ni_pop[ni_pop['Year']==2020].groupby('Age Band').sum()['Population'].astype(int).reset_index()
ni_pop = ni_pop.merge(age_bands_ons, how='inner', left_on='Age Band', right_on='Ages', validate='1:1')
ni_pop = ni_pop.groupby(['Order','Band']).sum()['Population'].reset_index()
ni_pop['% of total population'] = ni_pop['Population'] / ni_pop['Population'].sum()

# %% Load the 2020 population data for England and convert to the NI vaccine reporting bands
eng_pop = pandas.read_csv('eng_ons_pyramid.csv')
eng_pop = eng_pop[eng_pop['Year']==2020].groupby('Age Band').sum()['Population'].astype(int).reset_index()
eng_pop = eng_pop.merge(age_bands_ons, how='inner', left_on='Age Band', right_on='Ages', validate='1:1')
eng_pop = eng_pop.groupby(['Order','Band']).sum()['Population'].reset_index()
eng_pop['% of total population'] = eng_pop['Population'] / eng_pop['Population'].sum()

# %%
eng = pandas.read_excel('https://www.england.nhs.uk/statistics/wp-content/uploads/sites/2/2021/08/COVID-19-daily-announced-vaccinations-23-August-2021.xlsx', sheet_name='Vaccinations by LTLA and Age ', header=[11,12])
eng.dropna(axis='columns', how='all', inplace=True)
eng.dropna(axis='index', how='all', inplace=True)
newcols = [clean_cols(i) for i in eng.columns.values]
eng.columns = newcols
eng.dropna(axis='index', subset=['UTLA Name'], inplace=True)
eng = eng.drop(columns=['UTLA Code','UTLA Name','LTLA Code','LTLA Name','Total 1st Doses','Total 2nd Doses','Cumulative Total Doses (1st and 2nd doses) to Date'])
eng = eng.transpose().reset_index()
eng[['Dose','Age Band']] = eng['index'].str.split('_',1,expand=True)
eng.drop(columns=['index'],inplace=True)
eng = eng.set_index(['Dose','Age Band'])
eng = eng.fillna(0).sum(axis=1)
eng.name = 'Total'
eng = eng.reset_index()
eng['Total'] = eng['Total'].astype(int)
eng = eng[eng['Dose']=='1st dose'][['Age Band', 'Total']]
eng = eng.merge(age_bands_eng, how='inner', left_on='Age Band', right_on='Eng bands', validate='1:1')
eng = eng.groupby(['Band']).sum()['Total'].reset_index()
eng = eng.merge(eng_pop, how='inner', left_on='Band', right_on='Band', validate='1:1')
eng['Nation'] = 'England'

# %%
altair.Chart(eng).mark_tick().encode(
    x='Age Band:O',
    y='Total:Q',
    color='Dose'
)

# %%
ni = pandas.DataFrame([
    {'Age Band': 'Under 18', 'Dose': '1st dose', 'Total': 0},
    {'Age Band': '18-29', 'Dose': '1st dose', 'Total': 163796},
    {'Age Band': '30-39', 'Dose': '1st dose', 'Total': 174430},
    {'Age Band': '40-49', 'Dose': '1st dose', 'Total': 201081},
    {'Age Band': '50-59', 'Dose': '1st dose', 'Total': 233167},
    {'Age Band': '60-69', 'Dose': '1st dose', 'Total': 195974},
    {'Age Band': '70-79', 'Dose': '1st dose', 'Total': 143380},
    {'Age Band': '80+', 'Dose': '1st dose', 'Total': 85167}
])
ni = ni.merge(ni_pop, how='right', left_on='Age Band', right_on='Band', validate='1:1')
ni = ni[['Band', 'Order', 'Total', 'Population', '% of total population']]
ni['Nation'] = 'Northern Ireland'

# %%
df = pandas.concat([ni, eng])
df['Percentage first doses'] = (df['Total']/df['Population']).clip(upper=1.0)
df['First doses as % of total population'] = df['Percentage first doses'] * df['% of total population']

# %%
plt = altair.concat(
    altair.Chart(df).mark_bar(
        thickness=2,
        width=25,
        opacity=1
    ).encode(
        x=altair.X('Nation:O', axis=altair.Axis(labelAngle=0)),
        y=altair.Y('First doses as % of total population:Q', aggregate='sum', axis=altair.Axis(format='%', title='Population received first dose')),
        color=altair.Color('Nation', legend=None)
    ).properties(
        width=300,
        title=altair.TitleParams(
            text='NI has vaccinated 5% less of its population than England',
            subtitle=['NI has vaccinated 63%, England 68.2% for first doses'],
            align='left',
            anchor='start',
            fontSize=18,
            subtitleFontSize=14
        )
    )
).properties(
    title=altair.TitleParams(
        ['Population data for 2020 from ONS',
        'Vaccination data from HSCNI and NHS England',
        'https://twitter.com/ni_covid19_data on 26th July 2021'],
        baseline='bottom',
        orient='bottom',
        anchor='end',
        fontWeight='normal',
        fontSize=10,
        dy=10
    ),
)
plt.save('ni-eng-vacc-age-1-%s.png'%datetime.datetime.now().date().strftime('%Y-%d-%m'))
plt


# %%
plt = altair.concat(
    altair.Chart(df).mark_bar().encode(
        x=altair.X('Nation:O', axis=None),
        y=altair.Y('% of total population:Q', axis=altair.Axis(format='%', title='Age band % of total population')),
        color='Nation',
        column=altair.Column('Band:O', sort=altair.SortField('Order'), header=altair.Header(title='Age Band', labelOrient='bottom', titleOrient='bottom'), spacing=0)
    ).properties(
        width=50,
        title=altair.TitleParams(
            text='NI has a higher proportion of under 18s than England',
            subtitle=['This means that higher vaccine uptake is required in NI adults to provide',
                'the same level of protection to the whole population'],
            align='left',
            anchor='start',
            fontSize=18,
            subtitleFontSize=14
        )
    )
).properties(
    title=altair.TitleParams(
        ['Population data for 2020 from ONS',
        'https://twitter.com/ni_covid19_data on 26th July 2021'],
        baseline='bottom',
        orient='bottom',
        anchor='end',
        fontWeight='normal',
        fontSize=10,
        dy=10
    ),
)
plt.save('ni-eng-vacc-age-2-%s.png'%datetime.datetime.now().date().strftime('%Y-%d-%m'))
plt

# %%
plt = altair.concat(
    altair.Chart(df).mark_bar(
        thickness=2,
        width=25,
        opacity=1
    ).encode(
        x=altair.X('Nation:O', axis=None),
        y=altair.Y('Percentage first doses:Q', axis=altair.Axis(format='%', title='First doses completed')),
        color='Nation',
        column=altair.Column('Band:O', sort=altair.SortField('Order'), header=altair.Header(title='Age Band', labelOrient='bottom', titleOrient='bottom'), spacing=0)
    ).properties(
        width=50,
        title=altair.TitleParams(
            text='NI first dose uptake is lower for adults aged 18-60',
            subtitle='The gap is widest in the 30-39 age band',
            align='left',
            anchor='start',
            fontSize=18,
            subtitleFontSize=15
        )
    )
).properties(
    title=altair.TitleParams(
        ['Population data for 2020 from ONS',
        'Vaccination data from HSCNI and NHS England',
        'https://twitter.com/ni_covid19_data on 26th July 2021'],
        baseline='bottom',
        orient='bottom',
        anchor='end',
        fontWeight='normal',
        fontSize=10,
        dy=10
    ),
)
plt.save('ni-eng-vacc-age-3-%s.png'%datetime.datetime.now().date().strftime('%Y-%d-%m'))
plt


# %% [markdown]
# # Highest potential for vaccination
#
# * BT34 - Newry (South Down)
# * BT71 - Dungannon
# * BT7 - South Belfast (Ormeau)
# * BT9 - South Belfast (Malone)
# * BT35 - Newry (South Armagh)
# * BT66 - Craigavon (Derryadd)
# * BT62 - Craigavon (Portadown)
# * BT47 - Derry (Waterside)
# * BT48 - Derry (Cityside)
# * BT12 - West Belfast
