# %%
import pandas
import altair
from plot_shared import plot_points_average_and_trend
import datetime

# %%
def load_grouped_time_series(df, date_col, group_col, series_col, new_name, model=True):
    df = df.pivot(index=date_col,columns=group_col,values=series_col)
    newind = pandas.date_range(start=df.index.min(), end=df.index.max())
    df = df.reindex(newind)
    df = df.fillna(0)
    df = df.reset_index().melt(id_vars='index', var_name=group_col, value_name=series_col)
    df = df.rename(columns={'index': 'Date'}).sort_values('Date')
    df['%s 7-day rolling mean' %new_name] = df.groupby(group_col).rolling(7).mean().droplevel(0)
    if model is True:
        df = create_models(df, group_col, '%s 7-day rolling mean' %new_name)
    return df

# %%
adm_band_mapping = pandas.DataFrame({'Age Band': ['Aged 0 - 19', 'Aged 40 - 49', 'Aged 50 - 59', 'Aged 60 - 69',
    'Aged 70 - 79', 'Aged 80 & Over', 'Unknown', 'Aged 20 - 39'], 'Group': ['0 - 19', '20+', '20+', '20+', '20+', '20+', 'Unknown', '20+']})
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
        ]
    })

# %%
admissions = pandas.read_excel('https://www.health-ni.gov.uk/sites/default/files/publications/health/doh-dd-081021.xlsx', sheet_name='Admissions')
admissions = admissions.groupby(['Admission Date', 'Age Band'])['Number of Admissions'].sum().reset_index()
admissions['Admission Date'] = pandas.to_datetime(admissions['Admission Date'])
admissions = load_grouped_time_series(admissions, 'Admission Date', 'Age Band', 'Number of Admissions', 'Admissions', False)
admissions = admissions.merge(adm_band_mapping, how='left', on='Age Band')
admissions['Admissions 7-day rolling'] = admissions['Admissions 7-day rolling mean'] * 7

# %%
altair.Chart(admissions[admissions['Date'] > '2021-07-01']).mark_line().encode(
    x = 'Date:T',
    y = altair.Y(
        field='Admissions 7-day rolling mean',
        type='quantitative',
        aggregate='sum',
        axis=altair.Axis(title='Admissions per day (7 day average)'),
    ),
    color = 'Group:N'
)

# %%
cases = pandas.read_csv('agebands.csv')
cases = cases.merge(case_band_mapping, how='left', on='Age_Band_5yr')
overlay = cases[cases['Date'] == cases['Date'].max()]
overlay['Nearest'] = ((overlay['Positive_Tests']/overlay['Positive_Tests'].max()) * 40).astype(int) * (overlay['Positive_Tests'].max() / 40)
#overlay['Nearest'] = overlay['Nearest'].where(overlay['Band Start'] == 5)
overlay.loc[overlay['Band Start'] == 0, 'Nearest'] = 220
overlay.loc[overlay['Band Start'] == 5, 'Nearest'] = 900
overlay.loc[overlay['Band Start'] == 20, 'Nearest'] = 300
overlay.loc[overlay['Band Start'] == 25, 'Nearest'] = 380
overlay.loc[overlay['Band Start'] == 30, 'Nearest'] = 460
overlay.loc[overlay['Band Start'] == 40, 'Nearest'] = 680
overlay.loc[overlay['Band Start'] == 50, 'Nearest'] = 420
overlay.loc[overlay['Band Start'] == 55, 'Nearest'] = 340
overlay.loc[overlay['Band Start'] == 60, 'Nearest'] = 260
overlay.loc[overlay['Band Start'] == 65, 'Nearest'] = 90
overlay.loc[overlay['Band Start'] == 75, 'Nearest'] = 50

trend = altair.Chart(cases).mark_line().encode(
    x = 'Date:T',
    y = altair.Y(
        field='Positive_Tests',
        type='quantitative',
        aggregate='sum',
        axis=altair.Axis(title='Cases per day (7 day total)'),
    ),
    color = altair.Color(
        'Age_Band_5yr:N',
        sort=altair.EncodingSortField('Band Start'),
        legend=None
    ),
)

text = altair.Chart(overlay).mark_text(
        align='left',
        baseline='middle',
        dx=5
    ).encode(
        x = 'Date:T',
        y = altair.Y(
            field='Nearest',
            type='quantitative',
            aggregate='sum',
            axis=altair.Axis(title='Cases per day (7 day total)'),
        ),
        color = altair.Color(
            'Age_Band_5yr:N',
            sort=altair.EncodingSortField('Band Start'),
            legend=None
        ),
        text = altair.Text('Age_Band_5yr')
    )

plt = altair.concat(
    altair.layer(
        trend,
        text
    ).properties(
        height=450,
        width=800,
        title=altair.TitleParams(
            [
                'From DoH daily data',
                'https://twitter.com/ni_covid19_data on %s'  %datetime.datetime.now().date().strftime('%A %-d %B %Y'),
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
        'NI COVID-19 cases by age band',
        anchor='middle',
    )
)
plt.save('ni-age-band-cases-%s.png'%(datetime.datetime.now().date().strftime('%Y-%m-%d')))
plt

# %%
cases_total = cases.groupby(['Date','Age_Band_5yr'])['Positive_Tests'].sum()
cases_pc = cases_total.div(cases.groupby('Date')['Positive_Tests'].sum(),level='Date').reset_index()

altair.Chart(
    cases_pc
).mark_area().encode(
    x = 'Date:T',
    y = altair.Y('Positive_Tests:Q', axis=altair.Axis(format='%', title='% of cases', orient="right")),
    color='Age_Band_5yr'
)


# %%
altair.Chart(cases[cases['Date'] > '2021-07-01']).mark_line().encode(
    x = 'Date:T',
    y = altair.Y(
        field='Positive_Tests',
        type='quantitative',
        aggregate='sum',
        axis=altair.Axis(title='Cases per day (7 day total)'),
    ),
    color = 'Group:N'
)

# %%
altair.Chart(cases).mark_line().encode(
    x = 'Date:T',
    y = altair.Y(
        field='Positive_Tests',
        type='quantitative',
        aggregate='sum',
        axis=altair.Axis(title='Cases per day (7 day total)'),
    ),
    facet = altair.Facet(
        'Age_Band_5yr',
        columns=6
    )
).properties(
    height=100,
    width=100
)

# %%


# %%
plt = plot_points_average_and_trend(
    [
        {
            'points': None,
            'line': cases.set_index(['Date','Group'])['Positive_Tests'],
            'colour': 'Group',
            'date_col': 'Date',
            'x_title': 'Date',
            'y_title': 'New cases (7-day total)',
            'scales': ['linear'],
            'height': 225,
            'width': 400,
        },
        {
            'points': None,
            'line': admissions[admissions['Date'] >= cases['Date'].min()].set_index(['Date','Group'])['Admissions 7-day rolling'],
            'colour': 'Group',
            'date_col': 'Date',
            'x_title': 'Date',
            'y_title': 'New admissions (7-day total)',
            'scales': ['linear'],
            'height': 225,
            'width': 400,
        },
    ],
    '%s COVID-19 %s' %(
        'NI',
        'cases/admissions by age group',
    ),
    [
        'Data from DoH daily release',
        'Last two days cases, five days admissions likely to be revised upwards due to reporting delays',
        'https://twitter.com/ni_covid19_data on %s' % datetime.datetime.today().strftime('%A %-d %B %Y')
    ]
)
plt.save('ni-under-over-20-%s.png'%(datetime.datetime.now().date().strftime('%Y-%m-%d')))
plt