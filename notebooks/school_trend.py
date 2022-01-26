# %%
import pandas
import altair
import numpy
from plot_shared import plot_points_average_and_trend
from data_shared import get_ni_pop_pyramid
import datetime

# %%
def load_grouped_time_series(df, date_col, group_col, series_col, new_name, model=True, interpolate=False):
    df = df.pivot(index=date_col,columns=group_col,values=series_col)
    newind = pandas.date_range(start=df.index.min(), end=df.index.max())
    df = df.reindex(newind)
    if interpolate:
        df = df.interpolate()
    else:
        df = df.fillna(0)
    df = df.reset_index().melt(id_vars='index', var_name=group_col, value_name=series_col)
    df = df.rename(columns={'index': 'Date'}).sort_values('Date')
    df['%s 7-day rolling mean' %new_name] = df.groupby(group_col).rolling(7).mean().droplevel(0)
    if model is True:
        df = create_models(df, group_col, '%s 7-day rolling mean' %new_name)
    return df

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
        ],
        'Admissions Group': [
            'Aged 0 - 19', 'Aged 0 - 19', 'Aged 0 - 19', 'Aged 20 - 39',
            'Aged 20 - 39', 'Aged 20 - 39', 'Aged 20 - 39', 'Aged 40 - 49',
            'Aged 40 - 49', 'Aged 0 - 19', 'Aged 50 - 59', 'Aged 50 - 59',
            'Aged 60 - 69', 'Aged 60 - 69', 'Aged 70 - 79', 'Aged 70 - 79',
            'Aged 80 & Over', 'Unknown'
        ]
    })

# %%
admissions = pandas.read_excel('https://www.health-ni.gov.uk/sites/default/files/publications/health/doh-dd-260122.xlsx', sheet_name='Admissions')
admissions = admissions.groupby(['Admission Date', 'Age Band'])['Number of Admissions'].sum().reset_index()
admissions['Admission Date'] = pandas.to_datetime(admissions['Admission Date'])
admissions = load_grouped_time_series(admissions, 'Admission Date', 'Age Band', 'Number of Admissions', 'Admissions', False)
admissions = admissions.merge(adm_band_mapping, how='left', on='Age Band')
admissions['Admissions 7-day rolling'] = admissions['Admissions 7-day rolling mean'] * 7

# %%
altair.Chart(admissions[(admissions['Group'] == '0 - 19')]).mark_line().encode(
    x = 'Date:T',
    y = altair.Y(
        field='Admissions 7-day rolling mean',
        type='quantitative',
        aggregate='sum',
        axis=altair.Axis(title='Admissions per day (7 day average)'),
    ),
    color = 'Group:N'
).properties(
    height=450,
    width=800
)

# %%
admissions_grp = admissions.groupby(['Date','Age Band'])['Admissions 7-day rolling mean'].sum()
admissions_pc = admissions_grp/admissions_grp.groupby(level=0).sum()
admissions_pc = pandas.DataFrame(admissions_pc).reset_index()
altair.vconcat(
altair.Chart(
    admissions[admissions['Date'] > (admissions['Date'].max() + pandas.DateOffset(-42))]
).mark_line().encode(
    x = 'Date:T',
    y = altair.Y(
        field='Admissions 7-day rolling mean',
        type='quantitative',
        aggregate='sum',
        axis=altair.Axis(title='Admissions per day (7 day average)'),
    ),
    color = 'Age Band:N'
).properties(
    height=225,
    width=800
),
altair.Chart(
    admissions_pc[admissions_pc['Date'] > (admissions['Date'].max() + pandas.DateOffset(-42))]
).mark_area().encode(
    x = 'Date:T',
    y = altair.Y(
        field='Admissions 7-day rolling mean',
        type='quantitative',
        aggregate='sum',
        axis=altair.Axis(title='% admissions per day'),
    ),
    color = 'Age Band:N'
).properties(
    height=225,
    width=800
)
)


# %%
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
#overlay['Nearest'] = overlay['Nearest'].where(overlay['Band Start'] == 5)
#overlay.loc[overlay['Band Start'] == 0, 'Nearest'] = 220
#overlay.loc[overlay['Band Start'] == 5, 'Nearest'] = 900
#overlay.loc[overlay['Band Start'] == 20, 'Nearest'] = 300
#overlay.loc[overlay['Band Start'] == 25, 'Nearest'] = 380
#overlay.loc[overlay['Band Start'] == 30, 'Nearest'] = 460
#overlay.loc[overlay['Band Start'] == 40, 'Nearest'] = 680
#overlay.loc[overlay['Band Start'] == 50, 'Nearest'] = 420
#overlay.loc[overlay['Band Start'] == 55, 'Nearest'] = 340
#overlay.loc[overlay['Band Start'] == 60, 'Nearest'] = 260
#overlay.loc[overlay['Band Start'] == 65, 'Nearest'] = 90
#overlay.loc[overlay['Band Start'] == 75, 'Nearest'] = 50

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
def plot_timelines_with_latest(df, x, y, color, y_title, y_format, latest, latest_y, title, subtitle, y_scale='linear'):
    if y_scale == 'log':
        y_title += ' (log scale)'
        title += ' (log scale)'

    trend = altair.Chart(df).mark_line().encode(
        x = x,
        y = altair.Y(
            field=y,
            type='quantitative',
            aggregate='sum',
            axis=altair.Axis(title=y_title, format=y_format),
            scale=altair.Scale(type=y_scale),
        ),
        color = altair.Color(
            color,
            legend=None
        ),
    )

    text = altair.Chart(latest).mark_text(
        align='left',
        baseline='middle',
        dx=5
    ).encode(
        x = x,
        y = altair.Y(
            field=latest_y,
            type='quantitative',
            aggregate='sum',
            scale=altair.Scale(type=y_scale),
        ),
        color = altair.Color(
            color,
            legend=None
        ),
        text = altair.Text(color)
    )

    return altair.concat(
        altair.layer(
            trend,
            text
        ).properties(
            height=450,
            width=800,
            title=altair.TitleParams(
                subtitle,
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
            title,
            anchor='middle',
        )
    )

cases_broad = cases.groupby(['Date','Broad Group']).sum().reset_index()
cases_broad['Positive per 100k'] = (100000 * cases_broad['Positive_Tests']) / cases_broad['Population']
cases_broad['Tests per 100k'] = (100000 * cases_broad['Total_Tests']) / cases_broad['Population']
cases_broad['Positivity_Rate'] = cases_broad['Positive_Tests'] / cases_broad['Total_Tests']
overlay = cases_broad[cases_broad['Date'] == cases_broad['Date'].max()]
overlay['Nearest'] = overlay['Positive per 100k']
overlay['Nearest_PR'] = overlay['Positivity_Rate']
overlay['Nearest_Tests'] = overlay['Tests per 100k']

plt = plot_timelines_with_latest(
    cases_broad,
    'Date:T',
    'Positive per 100k',
    'Broad Group:N',
    'Positive per 100k, 7 day total',
    ',.2r',
    overlay,
    'Nearest',
    'NI COVID-19 positive cases per 100k people by age group',
    [
        'From DoH daily data',
        'https://twitter.com/ni_covid19_data on %s'  %datetime.datetime.now().date().strftime('%A %-d %B %Y'),
    ]
)
plt.save('ni-age-band-100k-cases-%s.png'%(datetime.datetime.now().date().strftime('%Y-%m-%d')))
plt

# %%
plt = plot_timelines_with_latest(
    cases_broad,
    'Date:T',
    'Positivity_Rate',
    'Broad Group:N',
    'Positivity Rate, 7-day average',
    '%',
    overlay,
    'Nearest_PR',
    'NI COVID-19 7-day positivity rate by age group',
    [
        'From DoH daily data',
        'https://twitter.com/ni_covid19_data on %s'  %datetime.datetime.now().date().strftime('%A %-d %B %Y'),
    ]
)
plt.save('ni-age-band-pr-%s.png'%(datetime.datetime.now().date().strftime('%Y-%m-%d')))
plt

# %%
plt = plot_timelines_with_latest(
    cases_broad,
    'Date:T',
    'Tests per 100k',
    'Broad Group:N',
    'Total Tests, 7-day total',
    ',.2r',
    overlay,
    'Nearest_Tests',
    'NI COVID-19 7-day total tests by age group',
    [
        'From DoH daily data',
        'https://twitter.com/ni_covid19_data on %s'  %datetime.datetime.now().date().strftime('%A %-d %B %Y'),
    ]
)
plt.save('ni-age-band-tests-%s.png'%(datetime.datetime.now().date().strftime('%Y-%m-%d')))
plt

# %%
overlay['One in N'] = 100000 / overlay['Positive per 100k']
overlay

# %%
cases_10yr = cases.groupby(['Date','10 Year Group']).sum().reset_index()
cases_10yr['Positive per 100k'] = (100000 * cases_10yr['Positive_Tests']) / cases_10yr['Population']
cases_10yr['Tests per 100k'] = (100000 * cases_10yr['Total_Tests']) / cases_10yr['Population']
cases_10yr['Positivity_Rate'] = cases_10yr['Positive_Tests'] / cases_10yr['Total_Tests']
cases_10yr['Date'] = pandas.to_datetime(cases_10yr['Date'])
cases_10yr['Cumulative Positive per 100k'] = cases_10yr.groupby('10 Year Group')['Positive per 100k'].cumsum() / 7.0
overlay_10yr = cases_10yr[cases_10yr['Date'] == cases_10yr['Date'].max()]
overlay_10yr['Nearest'] = overlay_10yr['Positive per 100k']
overlay_10yr = overlay_10yr[overlay_10yr['Nearest'] < 100000]
#overlay_10yr.loc[overlay_10yr['10 Year Group'] == '30 - 39', 'Nearest'] = 2650
#overlay_10yr.loc[overlay_10yr['10 Year Group'] == '15 - 19', 'Nearest'] = 2500
#overlay_10yr.loc[overlay_10yr['10 Year Group'] == '5 - 9', 'Nearest'] = 890

for scale in ['linear','log']:
    plt = plot_timelines_with_latest(
        cases_10yr[
            (cases_10yr['Positive per 100k'] > 0) &
            (cases_10yr['Positive per 100k'] < 100000) &
            (~cases_10yr['Positive per 100k'].isna()) &
            (cases_10yr['Date'] > (cases_10yr['Date'].max() + pandas.DateOffset(days=-42)))
        ],
        'Date:T',
        'Positive per 100k',
        '10 Year Group:N',
        'Positive per 100k, 7 day total',
        ',.2r',
        overlay_10yr,
        'Nearest',
        'NI COVID-19 positive cases per 100k people by age group, last six weeks',
        [
            'From DoH daily data',
            'https://twitter.com/ni_covid19_data on %s'  %datetime.datetime.now().date().strftime('%A %-d %B %Y'),
        ],
        y_scale=scale
    )
    plt.save('ni-10yr-age-band-%s-cases-%s.png'%(scale, datetime.datetime.now().date().strftime('%Y-%m-%d')))
plt

# %%
overlay_10yr_sept = cases_10yr[cases_10yr['Date'] == '2021-09-30']
overlay_10yr_sept['Nearest'] = overlay_10yr_sept['Positive per 100k']
overlay_10yr_sept = overlay_10yr_sept[overlay_10yr_sept['Nearest'] < 100000]
for scale in ['linear','log']:
    plt = plot_timelines_with_latest(
        cases_10yr[
            (cases_10yr['Positive per 100k'] > 0) &
            (cases_10yr['Positive per 100k'] < 100000) &
            (~cases_10yr['Positive per 100k'].isna()) &
            (cases_10yr['Date'] > '2021-08-20') &
            (cases_10yr['Date'] < '2021-10-01')
        ],
        'Date:T',
        'Positive per 100k',
        '10 Year Group:N',
        'Positive per 100k, 7 day total',
        ',.2r',
        overlay_10yr_sept,
        'Nearest',
        'NI COVID-19 positive cases per 100k people by age group, six weeks to end of Sept 2021',
        [
            'From DoH daily data',
            'https://twitter.com/ni_covid19_data on %s'  %datetime.datetime.now().date().strftime('%A %-d %B %Y'),
        ],
        y_scale=scale
    )
    plt.save('ni-10yr-sept-age-band-%s-cases-%s.png'%(scale, datetime.datetime.now().date().strftime('%Y-%m-%d')))
plt

# %%
overlay_adm = admissions[admissions['Date'] == admissions['Date'].max()]
overlay_adm['Nearest'] = overlay_adm['Admissions 7-day rolling mean']
for scale in ['linear','log']:
    plt = plot_timelines_with_latest(
        admissions[admissions['Date'] > (admissions['Date'].max() + pandas.DateOffset(-42))],
        'Date:T',
        'Admissions 7-day rolling mean',
        'Age Band:N',
        'Admissions per day, 7-day average',
        ',.2r',
        overlay_adm,
        'Nearest',
        'NI COVID-19 hospital admissions people by age group, last six weeks',
        [
            'From DoH daily data, last 5 days will likely be corrected upwards',
            'https://twitter.com/ni_covid19_data on %s'  %datetime.datetime.now().date().strftime('%A %-d %B %Y'),
        ],
        y_scale=scale
    )
    plt.save('ni-10yr-adm-age-band-%s-cases-%s.png'%(scale, datetime.datetime.now().date().strftime('%Y-%m-%d')))
plt


# %%
overlay_10yr['One in N'] = 100000 / overlay_10yr['Positive per 100k']
overlay_10yr

# %%
cases['Date'] = pandas.to_datetime(cases['Date'])
overlay_cases = cases[cases['Date'] == cases['Date'].max()]
overlay_cases['Nearest'] = overlay_cases['Positive per 100k']
#overlay_cases.loc[overlay_cases['Age_Band_5yr'] == 'Aged 75 - 79', 'Nearest'] = 110
#overlay_cases.loc[overlay_cases['Age_Band_5yr'] == 'Aged 70 - 74', 'Nearest'] = 100
#overlay_cases.loc[overlay_cases['Age_Band_5yr'] == 'Aged 80 & Over', 'Nearest'] = 115
for scale in ['linear','log']:
    plt = plot_timelines_with_latest(
        cases[(cases['Date'] > (cases['Date'].max() + pandas.DateOffset(days=-42))) & (cases['Age_Band_5yr'].isin(['Aged 60 - 64', 'Aged 65 - 69', 'Aged 70 - 74', 'Aged 75 - 79', 'Aged 80 & Over']))],
        'Date:T',
        'Positive per 100k',
        'Age_Band_5yr:N',
        'Positive per 100k, 7 day total',
        ',.2r',
        overlay_cases[(overlay_cases['Age_Band_5yr'].isin(['Aged 60 - 64', 'Aged 65 - 69', 'Aged 70 - 74', 'Aged 75 - 79', 'Aged 80 & Over']))],
        'Nearest',
        'NI COVID-19 positive cases per 100k people by older age group, last six weeks',
        [
            'From DoH daily data',
            'https://twitter.com/ni_covid19_data on %s'  %datetime.datetime.now().date().strftime('%A %-d %B %Y'),
        ],
        y_scale=scale
    )
    plt.save('ni-5yr-%s-age-band-cases-older-%s.png'%(scale,datetime.datetime.now().date().strftime('%Y-%m-%d')))
plt

# %%
mydf = pandas.DataFrame({
    'Date': ['2021-01-04','2021-01-04','2021-01-05','2021-01-05'],
    'Group': ['A','B','A','B'],
    'Col1': [0,1,2,3],
    'Col2': [4,1,5,3],
})
mydf.groupby('Group').corrwith(mydf.groupby('Group')['Col1'].shift(1))


# %%
cases_adm = cases.groupby(['Date','Admissions Group']).sum().reset_index()
cases_adm['Positivity_Rate'] = cases_adm['Positive_Tests'] / cases_adm['Total_Tests']
cases_adm['Date'] = pandas.to_datetime(cases_adm['Date'])
cases_adm = load_grouped_time_series(cases_adm, 'Date', 'Admissions Group', 'Positive_Tests', 'Positive_Tests', False, True)
cases_adm = admissions[admissions['Date'] >= cases_adm['Date'].min()].merge(cases_adm, how='left', right_on=['Date','Admissions Group'], left_on=['Date','Age Band'])
base = cases_adm.groupby('Admissions Group')[['Admissions 7-day rolling','Positive_Tests']]
corrs = pandas.DataFrame()
for offset in range(21):
    a = base.corrwith(cases_adm.groupby('Admissions Group')['Positive_Tests'].shift(offset)).reset_index()
    out = a[['Admissions Group','Admissions 7-day rolling']].rename(columns={'Admissions 7-day rolling': 'corr'})
    out['Offset'] = offset
    corrs = pandas.concat([corrs, out])
offsets = corrs.sort_values('corr').drop_duplicates('Admissions Group',keep='last').sort_values('Admissions Group')

altair.Chart(
    corrs
).mark_line().encode(
    x = 'Offset:Q',
    y = altair.Y('corr:Q'),
    color='Admissions Group'
)

# %% Exponential fitting functions
def variable_shift(x, df):
    part = df[df['Admissions Group']==x['Admissions Group']]
    part['Admissions 7-day rolling'] = part['Admissions 7-day rolling'].shift(-x['Offset'])
    return part

def get_model_for_area(df, to_model, x):
    df = df[(~df[x].isna()) & (~df[to_model].isna())]
    return numpy.polyfit(df[x], df[to_model], 1)

def fit_lin(model0, model1, value):
    return ((model0 * value)  + model1)

def create_models(df, areakey, x, to_model):
    a = offsets.apply(variable_shift, axis=1, df=df)
    df = pandas.concat(a.to_list())
    model = df.groupby(areakey).apply(get_model_for_area, x=x, to_model=to_model)
    model = pandas.DataFrame(model.to_list(), index=model.index).reset_index()
    model.rename(columns={0: '%s model0'%to_model, 1: '%s model1'%to_model}, inplace=True)
    df = df.merge(
        model,
        left_on=[areakey],
        right_on=[areakey],
        validate='m:1'
    )
    df['%s modelled' %to_model] = fit_lin(df['%s model0'%to_model], df['%s model1'%to_model], df[x])
    return(df, model)

cases_adm_modelled, adm_model = create_models(cases_adm, 'Admissions Group', 'Positive_Tests', 'Admissions 7-day rolling')
cases_adm_modelled['Status'] = cases_adm_modelled['Admissions 7-day rolling'].isna()
cases_adm_modelled['Admissions 7-day rolling'] = cases_adm_modelled['Admissions 7-day rolling'].fillna(cases_adm_modelled['Admissions 7-day rolling modelled'])

# %%
altair.Chart(
    cases_adm_modelled
).mark_point().encode(
    x = 'Positive_Tests:Q',
    y = altair.Y('Admissions 7-day rolling:Q'),
    facet='Admissions Group',
    color='Status'
).resolve_scale(
    x='independent',
    y='independent',
)

# %%
modelled = cases_adm_modelled[cases_adm_modelled['Status']]
modelled['day'] = modelled.groupby("Admissions Group")["Date"].rank(method="first", ascending=True)
modelled.groupby('day')['Admissions 7-day rolling'].sum() / 7

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

# %%
cases
# %%
cases.groupby('Date').sum()[['Positive_Tests','Total_Tests']]
# %%
summ = pandas.read_excel('https://www.health-ni.gov.uk/sites/default/files/publications/health/doh-dd-081021.xlsx', sheet_name='Summary Tests')

# %%
plt = plot_timelines_with_latest(
    cases_10yr[cases_10yr['Date'] > (cases_10yr['Date'].max() + pandas.DateOffset(days=-42))],
    'Date:T',
    'Cumulative Positive per 100k',
    '10 Year Group:N',
    'Cumulative Positive per 100k',
    ',.2r',
    overlay_10yr,
    'Cumulative Positive per 100k',
    'NI COVID-19 cumulative positive cases per 100k people by age group (last six weeks)',
    [
        'From DoH daily data',
        'https://twitter.com/ni_covid19_data on %s'  %datetime.datetime.now().date().strftime('%A %-d %B %Y'),
    ]
)
plt.save('ni-10yr-age-band-cum-cases-%s.png'%(datetime.datetime.now().date().strftime('%Y-%m-%d')))
plt

# %%
overlay_10yr[['10 Year Group','Cumulative Positive per 100k']]
