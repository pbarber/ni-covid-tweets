# %%
import pandas
import altair
import datetime

# %% Load latest postcode cases data
dd = pandas.read_excel('~/Downloads/doh-dd-120821.xlsx', sheet_name='Tests by Postal District-7 days')
vacc = pandas.read_csv('postcodes.csv')
df = vacc.merge(dd, how='inner', left_on='Postcode District', right_on='Postal_District')
df = df[['Postcode District','Vaccinations','Rate per 100K Pop.','Population_y']].rename(columns={'Rate per 100K Pop.': 'Cases per 100k People'})
df['Vaccinations per Person'] = df['Vaccinations']/df['Population_y']

# %%
plt = altair.Chart(df).mark_point().encode(
    x=altair.X('Vaccinations per Person:Q'),
    y=altair.Y('Cases per 100k People:Q'),
    tooltip='Postcode District:N'
)
plt.show()
# %%
vacc['Population'].sum()
# %%
vacc['Vaccinations'].sum()

# %%
regional_centres = ['BT3','BT12','BT43','BT64','BT47','BT79','BT74']
mobile_clinics_all = {
    '2021-08-28': [ # From https://www.bbc.co.uk/news/uk-northern-ireland-57863840
        'BT7', 'BT5', 'BT15', 'BT11', 'BT12', 'BT5', # BHSCT
        'BT20', 'BT4', 'BT1', 'BT34', 'BT19', # SEHSCT
        'BT80', 'BT44', # NHSCT
        'BT35', 'BT61', 'BT71', # SHSCT
        'BT47', 'BT79', 'BT48', 'BT78', # WHSCT
        ]
    }
mobile_clinics = mobile_clinics_all['2021-08-28']
df = pandas.read_csv('postcodes.csv')

# %%
change = df[df['Date']=='2021-08-27'].merge(df[df['Date']=='2021-08-20'], how='inner', on='Postcode District', suffixes=('','_y'))
change['Change'] = change['Vaccinations']-change['Vaccinations_y']
change.columns
change = change[['Postcode District','Vaccinations','Population','Change','Potential vaccinations']]
change['Colour'] = 'None'
change.loc[change['Postcode District'].isin(regional_centres), 'Colour'] = 'Regional Centre'
change.loc[change['Postcode District'].isin(mobile_clinics), 'Colour'] = 'Mobile Clinic'

# %%
for order, name in {'Potential vaccinations': 'potential vaccinations','Change': 'weekly change'}.items():
    plt = altair.vconcat(
        altair.Chart(change).mark_bar().encode(
            y=altair.Y(
                'Postcode District:N',
                title='Postcode District (highest %s at top, lowest at bottom)' %name,
                sort=altair.SortField(
                    order,
                    'descending'
                )
            ),
            x=altair.X('Change:Q', title='New vaccinations this week'),
            color=altair.Color(
                'Colour',
                scale=altair.Scale(
                    range=['grey','blue','orange'],
                    domain=['None','Regional Centre','Mobile Clinic'],
                ),
                legend=altair.Legend(title='')
            ),
        ).properties(
            height=1000,
            width=450,
            title='NI COVID-19 Vaccinations last week by Postcode District'
        )
    ).properties(
        title=altair.TitleParams(
            ['Vaccinations data from HSCNI COVID-19 dashboard, mid-2018 populations from NISRA',
            'Mobile vaccination clinic locations for last week from BBC News NI',
            'https://twitter.com/ni_covid19_data on %s' %datetime.datetime.now().strftime('%A %-d %B %Y')],
            baseline='bottom',
            orient='bottom',
            anchor='end',
            fontWeight='normal',
            fontSize=10,
            dy=10
        ),
    )
    plt.save('vacc-weekly-postcode-%s-%s.png'%(order.lower().replace(' ','-'),datetime.datetime.now().date().strftime('%Y-%m-%d')))

# %%
change.dtypes
# %%
