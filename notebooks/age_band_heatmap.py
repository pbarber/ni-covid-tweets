# %% Imports
import pandas
import altair
import datetime
import boto3

from plot_shared import get_chrome_driver
from data_shared import get_s3_csv_or_empty_df

# %%
age_bands = pandas.read_excel('https://www.health-ni.gov.uk/sites/default/files/publications/health/doh-dd-030921.xlsx', sheet_name='Individuals 7 Days - 5yr Age')
age_bands['Total_Tests'] = age_bands['Positive_Tests'] + age_bands['Negative_Tests'] + age_bands['Indeterminate_Tests']
age_bands = age_bands.groupby('Age_Band_5yr').sum()[['Positive_Tests','Total_Tests']].reset_index()
age_bands['Positivity_Rate'] = age_bands['Positive_Tests'] / age_bands['Total_Tests']
age_bands['Band Start'] = age_bands['Age_Band_5yr'].str.extract('Aged (\d+)')
age_bands['Band End'] = age_bands['Age_Band_5yr'].str.extract('Aged \d+ - (\d+)')

# %%
#session = boto3.session.Session(profile_name='codeandnumbers')
#s3 = session.client('s3')
#datastore = get_s3_csv_or_empty_df(s3, 'ni-covid-tweets', 'DoH-DD/agebands.csv', ['Date'])
datastore = pandas.read_csv('../sam/agebands.csv')
datastore['Date'] = pandas.to_datetime(datastore['Date'])
datastore['Positive_Tests'] = datastore['Positive_Tests'].astype(int)
datastore['Total_Tests'] = datastore['Total_Tests'].astype(int)
datastore['Band Start'] = datastore['Band Start'].fillna(90).astype(int)
datastore = datastore.sort_values(['Date','Band Start']).reset_index(drop=True)
# Have to insert an extra date to get the first date shown - just altair things
#datastore = datastore.append(
#    {
#        'Date': datastore['Date'].min() + pandas.DateOffset(days=-1),
#        'Positive_Tests': 1,
#        'Total_Tests': 1,
#        'Positivity_Rate': 1,
#        'Age_Band_5yr': 'Not Known',
#        'Band Start': 90
#    }, ignore_index=True)

# %%
toplot = datastore[datastore['Date'] >= (datastore['Date'].max() + pandas.DateOffset(days=-42))]
toplot['Date'] = pandas.to_datetime(toplot['Date'])
newind = pandas.date_range(start=toplot['Date'].max() + pandas.DateOffset(days=-42), end=toplot['Date'].max())
alldates = pandas.Series(newind)
alldates.name = 'Date'
toplot = toplot.merge(alldates, how='outer', left_on='Date', right_on='Date')
toplot['X'] = toplot['Date'].dt.strftime('%e %b')
toplot['Most Recent Positive Tests'] = toplot['Positive_Tests'].where(toplot['Date'] == toplot['Date'].max()).apply(lambda x: f"{x:n}" if not pandas.isna(x) else "")
toplot['Age_Band_5yr'].fillna('Not Known', inplace=True)
ticks = 7
if len(toplot['Date'].unique()) < 7:
    ticks = len(toplot['Date'].unique())
heatmap = altair.Chart(toplot).mark_rect().encode(
    x = altair.X(
        field='X',
        type='ordinal',
        axis=altair.Axis(
            tickCount=ticks
        ),
        sort=altair.SortField(
            'Date'
        ),
        title='Date'
    ),
    y = altair.Y(
        field='Age_Band_5yr',
        type='ordinal',
        sort=altair.SortField(
            'Band Start'
        ),
        title='Age Band',
    ),
    color = altair.Color(
        field='Positive_Tests',
        type='quantitative',
        aggregate='sum',
        title='Positive Tests (7 day total)',
    )
).properties(
    height=450,
    width=800,
    title='NI COVID-19 Positive Tests by Age Band from %s to %s' %(toplot['Date'].min().strftime('%-d %B %Y'),toplot['Date'].max().strftime('%-d %B %Y'))
)

plt = altair.vconcat(
    altair.layer(
        heatmap,
        heatmap.mark_text(
            align='right',
            baseline='middle',
            dx=43
        ).encode(
            text = altair.Text('Most Recent Positive Tests'),
            color = altair.value('black')
        )
    )
).properties(
    title=altair.TitleParams(
        ['Data from DoH daily downloads',
        'Numbers to right of chart show most recent 7 day total',
        'https://twitter.com/ni_covid19_data on %s'  %datetime.datetime.now().strftime('%A %-d %B %Y')],
        baseline='bottom',
        orient='bottom',
        anchor='end',
        fontWeight='normal',
        fontSize=10,
        dy=10
    ),
)
plt

# %%

# %%
a = toplot[toplot['Age_Band_5yr']=='Aged 10 - 14'][['Date','Age_Band_5yr','Positive_Tests','Band Start']]
altair.Chart(a).mark_rect().encode(
    x = 'Date:O',
    y = 'Band Start:O',
    color = 'Positive_Tests',
)