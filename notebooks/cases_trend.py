# %% Imports
import pandas
import altair
import numpy
import datetime
from sklearn.linear_model import LinearRegression

# %% Exponential fitting functions
def calc_exp_fit0(data):
    curve = numpy.polyfit(data.index, numpy.log(data.values), 1)
    return curve[0]

def calc_exp_fit1(data):
    curve = numpy.polyfit(data.index, numpy.log(data.values), 1)
    return curve[1]

def get_model_for_area(df, to_model):
    df.set_index('x', inplace=True)
    df['%s model0'%to_model] = df[to_model].rolling(window=9, center=True).apply(calc_exp_fit0)
    df['%s model1'%to_model] = df[to_model].rolling(window=9, center=True).apply(calc_exp_fit1)
    return df[['%s model0'%to_model,'%s model1'%to_model]]

def create_models(df, areakey, to_model):
    df['x'] = (df['Date'] - df['Date'].min()).dt.days
    df = df.merge(
        df.groupby(areakey).apply(get_model_for_area, to_model=to_model),
        left_on=[areakey,'x'],
        right_index=True,
        validate='1:1'
    )
    df['%s model_daily_change' %to_model] = (fit_exp(df['%s model0'%to_model], df['%s model1'%to_model], 2) - fit_exp(df['%s model0'%to_model], df['%s model1'%to_model], 1)) / fit_exp(df['%s model0'%to_model], df['%s model1'%to_model], 1)
    df['%s model_weekly_change' %to_model] = (fit_exp(df['%s model0'%to_model], df['%s model1'%to_model], 8) - fit_exp(df['%s model0'%to_model], df['%s model1'%to_model], 1)) / fit_exp(df['%s model0'%to_model], df['%s model1'%to_model], 1)
    return(df)

def fit_exp(curve0, curve1, value):
    return (numpy.exp(curve1) * numpy.exp(curve0 * value))

def plot_single_trendline_fit(df, y, y_title, group, groupval, colour, totdays, fitdays, ignoredays=0):
    nofit_dates = df[(~df[y].isna()) & (df[group]==groupval)]['Date'].drop_duplicates().nlargest(totdays+ignoredays).nsmallest(totdays-fitdays)
    fit_dates = df[(~df[y].isna()) & (df[group]==groupval)]['Date'].drop_duplicates().nlargest(fitdays+ignoredays).nsmallest(fitdays)
    ignore_dates= df[(~df[y].isna()) & (df[group]==groupval)]['Date'].drop_duplicates().nlargest(ignoredays)
    nofit = altair.Chart(
        df[((df['Date'].isin(nofit_dates)) | df['Date'].isin(ignore_dates)) & (~df[y].isna()) & (df[group]==groupval)]
    ).mark_point(
        color=colour,
        opacity=0.7
    ).encode(
        x=altair.X(
            field='Date',
            type='temporal'
        ),
        y=altair.Y(
            field=y,
            type='quantitative',
            aggregate='sum',
            scale=altair.Scale(
                type='log'
            ),
        )
    )

    tofit = df[(df['Date'].isin(fit_dates)) & (~df[y].isna()) & (df[group]==groupval)]
    fit = altair.Chart(
        tofit
    ).mark_point(
        color=colour,
        opacity=0.7
    ).encode(
        x=altair.X(
            field='Date',
            type='temporal'
        ),
        y=altair.Y(
            field=y,
            type='quantitative',
            aggregate='sum',
            scale=altair.Scale(
                type='log'
            ),
            title=y_title
        )
    )

    tofit['x'] = (tofit['Date'] - df['Date'].min()).dt.days
    curve = numpy.polyfit(tofit['x'], numpy.log(tofit[y]), 1)
    tofit['result'] = fit_exp(curve[0], curve[1], tofit['x'])
    pct_change = (fit_exp(curve[0], curve[1], 2) - fit_exp(curve[0], curve[1], 1)) / fit_exp(curve[0], curve[1], 1)
    pct_change_wk = (fit_exp(curve[0], curve[1], 8) - fit_exp(curve[0], curve[1], 1)) / fit_exp(curve[0], curve[1], 1)
    model = pandas.DataFrame({
        'Date': tofit['Date'].mean().strftime('%A %-d %B %Y'),
        'Daily': abs(pct_change),
        'Weekly': abs(pct_change_wk),
        'RF': 'fall' if (pct_change < 0) else 'rise',
        'HD': 'Halving' if (curve[0] < 0) else 'Doubling',
        "HD_time": abs(numpy.log(2)/curve[0])
    }, index=[0])

    lobf = altair.Chart(
        tofit
    ).mark_line(
        color=colour,
        opacity=0.7
    ).encode(
        x=altair.X(
            field='Date',
            type='temporal',
            axis=altair.Axis(
                title='Specimen Date',
                format = ("%-d %b")
            )
        ),
        y=altair.Y(
            field='result',
            type='quantitative',
            aggregate='sum',
            scale=altair.Scale(
                type='log'
            ),
            title=y_title
        )
    )

    labels = altair.Chart(
        model
    ).transform_calculate(
        pct=f'"Daily " + datum.RF + ": " + format(datum.Daily,".1%")',
        pct_wk=f'"Weekly " + datum.RF + ": " + format(datum.Weekly,".1%")',
        hd=f'datum.HD + " time: " + format(datum.HD_time,".1f") + " days"',
        date=f'"Trendline mid-point: " + datum.Date'
    )

    return altair.concat(
        altair.layer(
            nofit,
            fit,
            lobf,
            labels.mark_text(align='left').encode(
                x=altair.value(20),
                y=altair.value(20),
                text='pct:O'
            ),
            labels.mark_text(align='left').encode(
                x=altair.value(20),
                y=altair.value(37),
                text='pct_wk:O'
            ),
            labels.mark_text(align='left').encode(
                x=altair.value(20),
                y=altair.value(54),
                text='hd:O'
            ),
            labels.mark_text(align='left').encode(
                x=altair.value(20),
                y=altair.value(71),
                text='date:O'
            ),
        ).properties(
            title=altair.TitleParams(
                'COVID-19 cases in %s' %groupval,
            ),
            height=384,
            width=512,
        )
    ).properties(
        title=altair.TitleParams(
            'https://twitter.com/ni_covid19_data',
            baseline='bottom',
            orient='bottom',
            anchor='end',
            fontWeight='normal',
            fontSize=10,
            dy=10
        ),
    )


# %% 2019 populations from Fig 3 of https://www.ons.gov.uk/peoplepopulationandcommunity/populationandmigration/populationestimates/bulletins/annualmidyearpopulationestimates/mid2020
nationpop = pandas.DataFrame([
    {'Nation': 'Northern Ireland', 'Population': 1895510},
    {'Nation': 'England', 'Population': 56550138},
    {'Nation': 'Scotland', 'Population': 5466000},
    {'Nation': 'Wales', 'Population': 3169586},
])

# %% NISRA mid-year LGD population estimates
nipop = pandas.DataFrame([
    {"Area": "Antrim and Newtownabbey","Population": 143504},
    {"Area": "Armagh City, Banbridge and Craigavon","Population": 216205},
    {"Area": "Belfast","Population": 343542},
    {"Area": "Causeway Coast and Glens","Population": 144838},
    {"Area": "Derry City and Strabane","Population": 151284},
    {"Area": "Fermanagh and Omagh","Population": 117397},
    {"Area": "Lisburn and Castlereagh","Population": 146002},
    {"Area": "Mid and East Antrim","Population": 139274},
    {"Area": "Ards and North Down","Population": 161725},
    {"Area": "Mid Ulster","Population": 148528},
    {"Area": "Newry, Mourne and Down","Population": 181368},
    {"Area": "Missing Postcode","Population": 0}
])

# %% Load data from PHE API and transform it to the right format
df = pandas.read_csv('https://coronavirus.data.gov.uk/api/v1/data?filters=areaType=nation&structure=%7B%22areaType%22:%22areaType%22,%22areaName%22:%22areaName%22,%22areaCode%22:%22areaCode%22,%22date%22:%22date%22,%22newCasesBySpecimenDate%22:%22newCasesBySpecimenDate%22,%22cumCasesBySpecimenDate%22:%22cumCasesBySpecimenDate%22%7D&format=csv')
df.drop(columns=['areaType','areaCode','cumCasesBySpecimenDate'], inplace=True)
df['date'] = pandas.to_datetime(df['date'], format='%Y-%m-%d').dt.date
df = df.pivot(index='date',columns='areaName',values='newCasesBySpecimenDate')
newind = pandas.date_range(start=df.index.min(), end=df.index.max())
df = df.reindex(newind)
df = df.reset_index().melt(id_vars='index', var_name='Nation', value_name='newCasesBySpecimenDate')
df = df.rename(columns={'index': 'Date'}).sort_values('Date')
df['New cases 7-day rolling mean'] = df.groupby('Nation').rolling(7, center=True).mean().droplevel(0)
df = df.merge(nationpop, how='left', left_on='Nation', right_on='Nation', validate='m:1')
df['Rolling cases per 100k'] = 100000 * (df['New cases 7-day rolling mean'] / df['Population'])
df['New cases per 100k'] = 100000 * (df['newCasesBySpecimenDate'] / df['Population'])
df = create_models(df, 'Nation', 'Rolling cases per 100k')
df = create_models(df, 'Nation', 'newCasesBySpecimenDate')

# %% Load NI regional data
ni = pandas.read_excel('~/Downloads/doh-dd-230721.xlsx', sheet_name='Tests')
ni.rename(columns={'LGD2014NAME': 'Area', 'Date of Specimen': 'Date'}, inplace=True)
ni['Area'] = ni['Area'].fillna('Missing Postcode')
newind = pandas.date_range(start=ni['Date'].min(), end=ni['Date'].max())
ni.fillna(0, inplace=True)
ni = ni.groupby(['Date','Area']).sum()
ni = ni.reindex(pandas.MultiIndex.from_product([newind,ni.index.levels[1]], names=['Date','Area']), fill_value=0).reset_index()
ni['New cases 7-day rolling mean'] = ni.groupby('Area')['Individ with Positive Lab Test'].rolling(7, center=True).mean().droplevel(0)
ni = ni.merge(nipop, how='left', left_on='Area', right_on='Area', validate='m:1')
ni['Rolling cases per 100k'] = 100000 * (ni['New cases 7-day rolling mean'] / ni['Population'])
ni = create_models(ni, 'Area', 'Rolling cases per 100k')

# %% Load NI admissions data
admissions = pandas.read_excel('~/Downloads/doh-dd-230721.xlsx',engine='openpyxl',sheet_name='Admissions')
admissions = admissions.groupby('Admission Date')['Number of Admissions'].sum().reset_index()
admissions.set_index('Admission Date', inplace=True)
newind = pandas.date_range(start=admissions.index.min(), end=admissions.index.max())
admissions = admissions.reindex(newind)
admissions.index.name = 'Admission Date'
admissions.reset_index(inplace=True)
admissions.fillna(0, inplace=True)
admissions['Number of Admissions 7-day rolling mean'] = admissions['Number of Admissions'].rolling(7, center=True).mean()

# %% LOad NI deaths data
deaths = pandas.read_excel('~/Downloads/doh-dd-230721.xlsx',engine='openpyxl',sheet_name='Deaths')
deaths = deaths.groupby('Date of Death')['Number of Deaths'].sum().reset_index()
deaths.set_index('Date of Death', inplace=True)
newind = pandas.date_range(start=deaths.index.min(), end=deaths.index.max())
deaths = deaths.reindex(newind)
deaths.index.name = 'Date of Death'
deaths.reset_index(inplace=True)
deaths.fillna(0, inplace=True)
deaths['Number of Deaths 7-day rolling mean'] = deaths['Number of Deaths'].rolling(7, center=True).mean()

# %%
covid_timeline = pandas.DataFrame([
    {'date':'2020-12-26','event':'Lockdown begins'},
    {'date':'2021-01-08','event':'Stay at home order'},
    {'date':'2021-03-08','event':'Some primary pupils return'},
    {'date':'2021-03-22','event':'All primary pupils return'},
    {'date':'2021-04-01','event':'Outdoor restrictions eased'},
    {'date':'2021-04-04','event':'Easter Sunday'},
    {'date':'2021-04-12','event':'All pupils return'},
    {'date':'2021-04-23','event':'Close contact services resume'},
    {'date':'2021-04-30','event':'Non-essential retail opens'},
])
covid_timeline['date'] = pandas.to_datetime(covid_timeline['date'])

def covid_timeline_ticks(height=200, clip=True, days=1000):
    return altair.Chart(
        covid_timeline[covid_timeline['date']>(covid_timeline['date'].max()-pandas.to_timedelta(days,unit='d'))]
    ).mark_tick(height=height, clip=clip).encode(
        x=altair.X('date:T')
    )

def covid_timeline_base_chart(days=1000):
    return altair.layer(
        covid_timeline_ticks(days=days),
        altair.Chart(
            covid_timeline[covid_timeline['date']>(covid_timeline['date'].max()-pandas.to_timedelta(days,unit='d'))]
        ).mark_text(
            dy=-5,
            angle=270
        ).encode(
            x=altair.X('date:T', axis=altair.Axis(title='Date')),
            text='event'
        ).properties(
            height=150,
            width=800
        )
    )

# %% Trendline functions
def plot_multiple_trendlines(df, y, color, y_scale=None, domain=None, range=None, y_format=',d', y_title='Title'):
    if y_scale == 'log':
        y_def = altair.Y(
            field=y,
            type='quantitative',
            scale=altair.Scale(
                type='log'
            ),
            axis=altair.Axis(
                format=y_format,
                title=y_title,
            ),
        )
    else:
        y_def = altair.Y(
            field=y,
            type='quantitative',
            axis=altair.Axis(
                format=y_format,
                title=y_title,
            ),
        )
    if domain is not None:
        c_def = altair.Color(
            field=color,
            type='nominal',
            scale=altair.Scale(
                domain=domain,
                range=range
            )
        )
    else:
        c_def = altair.Color(
            field=color,
            type='nominal'
        )

    return altair.Chart(
        df
    ).mark_line().encode(
        x=altair.X(
            field='Date',
            type='temporal',
        ),
        y=y_def,
        color=c_def
    ).properties(
        height=450,
        width=800,
        title=altair.TitleParams(
            'https://twitter.com/ni_covid19_data',
            baseline='bottom',
            orient='bottom',
            anchor='end',
            fontWeight='normal',
            fontSize=10,
            dy=10
        ),
    )

def plot_single_trendline(df, y, color, y_format=',', y_scale=None, x_axis=None, domain=None, range=None):
    if y_scale == 'log':
        y_def = altair.Y(
            field=y,
            type='quantitative',
            axis=altair.Axis(format=y_format),
            scale=altair.Scale(
                type='log'
            ),
        )
    else:
        y_def = altair.Y(
            field=y,
            type='quantitative',
            axis=altair.Axis(format=y_format),
        )

    return altair.Chart(
        df
    ).mark_line(
        color=color,
    ).encode(
        x=altair.X(
            field='Date',
            type='temporal',
            axis=x_axis,
        ),
        y=y_def
    )

# %% Plot the change in cases since Christmas for all UK nations
plot_multiple_trendlines(
    df[df['Date']>'2020-12-24'],
    y='New cases 7-day rolling mean',
    color='Nation',
    y_scale='log',
    y_title='New cases',
    domain=['England','Scotland','Wales','Northern Ireland'],
    range=['grey','#005eb8','#D30731','#076543']
)

# %% Plot the change in cases per 100k since Christmas for all UK nations
plt = plot_multiple_trendlines(
    df[(df['Date']>'2020-12-24') & (df['Date']<'2021-07-19')],
    y='Rolling cases per 100k',
    color='Nation',
    y_scale='linear',
    y_title='New cases per 100k (rolling average, log scale)',
    domain=['England','Scotland','Wales','Northern Ireland'],
    range=['grey','#005eb8','#D30731','#076543']
)
plt.save('uk-linear-%s.png'%datetime.datetime.now().date().strftime('%Y-%d-%m'))
plt

# %% Plot just NI cases since Christmas
altair.vconcat(
    *[
        altair.layer(
            covid_timeline_ticks(height=300),
            plot_single_trendline(
                df[(df['Date']>'2020-12-24') & (df['Nation']=='Northern Ireland')],
                y='New cases 7-day rolling mean',
                color='#076543',
                y_scale='log'
            )
        ),
        covid_timeline_base_chart()
    ],
    spacing=0
).resolve_scale(
    x='shared'
)

# %% Plot just NI cases per 100k since Christmas
altair.vconcat(
    *[
        altair.layer(
            covid_timeline_ticks(height=300),
            plot_single_trendline(
                df[(df['Date']>'2020-12-24') & (df['Nation']=='Northern Ireland')],
                y='Rolling cases per 100k',
                color='#076543',
                y_scale='log'
            )
        ),
        covid_timeline_base_chart()
    ],
    spacing=0
).resolve_scale(
    x='shared'
)

# %%
plt = plot_single_trendline_fit(df, 'Rolling cases per 100k', 'New cases per 100k (7-day rolling mean)', 'Nation', 'Northern Ireland', '#076543', 42, 9, 1)
plt.save('nir-%s.png'%datetime.datetime.now().date().strftime('%Y-%d-%m'))
plt

# %%
plt = plot_single_trendline_fit(df, 'Rolling cases per 100k', 'New cases per 100k (7-day rolling mean)', 'Nation', 'Wales', '#D30731', 42, 9, 1)
plt.save('wal-%s.png'%datetime.datetime.now().date().strftime('%Y-%d-%m'))
plt

# %%
plt = plot_single_trendline_fit(df, 'Rolling cases per 100k', 'New cases per 100k (7-day rolling mean)', 'Nation', 'Scotland', '#005eb8', 42, 9, 1)
plt.save('sco-%s.png'%datetime.datetime.now().date().strftime('%Y-%d-%m'))
plt

# %%
plt = plot_single_trendline_fit(df, 'Rolling cases per 100k', 'New cases per 100k (7-day rolling mean)', 'Nation', 'England', 'slategrey', 42, 9, 1)
plt.save('eng-%s.png'%datetime.datetime.now().date().strftime('%Y-%d-%m'))
plt

# %%
plot_multiple_trendlines(
    ni[ni['Date']>'2020-12-24'],
    y='New cases 7-day rolling mean',
    color='Area',
    y_scale='log',
).properties(
    height=450,
    width=800
)

# %%
plot_multiple_trendlines(
    ni[ni['Date']>'2020-12-24'],
    y='Rolling cases per 100k',
    color='Area',
    y_scale='log',
).properties(
    height=450,
    width=800
)

# %%
plot_multiple_trendlines(
    ni[ni['Date']>'2021-03-31'],
    y='Rolling cases per 100k',
    color='Area',
    y_scale='log',
).properties(
    height=450,
    width=800
)

# %%
plot_single_trendline_fit(ni, 'Rolling cases per 100k', 'New cases per 100k (7-day rolling mean)', 'Area', 'Derry City and Strabane', 'slategrey', 42, 9, 1)

# %%
plot_single_trendline_fit(ni, 'Rolling cases per 100k', 'New cases per 100k (7-day rolling mean)', 'Area', 'Armagh City, Banbridge and Craigavon', 'red', 42, 9, 1)

# %%
plot_single_trendline_fit(ni, 'Rolling cases per 100k', 'New cases per 100k (7-day rolling mean)', 'Area', 'Ards and North Down', 'orange', 42, 9, 1)

# %%
plot_single_trendline(
    ni[(ni['Date'] > '2020-12-24') & (ni['Area']=='Derry City and Strabane')],
    y='model_daily_change',
    color='#076543',
    y_format='%'
)

# %%
plot_single_trendline(
    ni[(ni['Date'] > '2020-12-24') & (ni['Area']=='Armagh City, Banbridge and Craigavon')],
    y='model_daily_change',
    color='#076543',
    y_format='%'
)

# %%
plot_multiple_trendlines(
    ni[(ni['Date']>'2020-11-24') & (~ni['model_daily_change'].isna())],
    y='model_daily_change',
    color='Area',
    y_format='%',
    y_title='Daily change in 7-day rolling average of cases',
).properties(
    height=450,
    width=800
)

# %%
plot_multiple_trendlines(
    ni[(ni['Date']>'2021-04-23') & (~ni['model_daily_change'].isna())],
    y='model_daily_change',
    color='Area',
    y_format='%',
    y_title='Daily change in 7-day rolling average of cases',
).properties(
    height=450,
    width=800
)

# %%
plot_multiple_trendlines(
    df[df['Date']>'2020-11-24'],
    y='Rolling cases per 100k model_daily_change',
    color='Nation',
    domain=['England','Scotland','Wales','Northern Ireland'],
    range=['grey','#005eb8','#D30731','#076543'],
    y_format='%',
    y_title='Daily change in 7-day rolling average of cases',
).properties(
    height=450,
    width=800
)

# %%
plt = plot_multiple_trendlines(
    df[(df['Date']>'2021-04-23') & (~df['Rolling cases per 100k model_daily_change'].isna())],
    y='Rolling cases per 100k model_daily_change',
    color='Nation',
    domain=['England','Scotland','Wales','Northern Ireland'],
    range=['grey','#005eb8','#D30731','#076543'],
    y_format='%',
    y_title='Daily change in 7-day rolling average of cases',
).properties(
    height=450,
    width=800
)
plt.save('nations-daily-change-%s.png'%datetime.datetime.now().date().strftime('%Y-%d-%m'))
plt

def points_average_and_trend(points, line, colour, date_col, x_title, y_title, scale='linear', width=800, height=450, x_type='temporal', colour_domain=[], colour_range=[]):
    if scale=='log':
        y_title += ' (log scale)'
        line_df = line[(~line.isna()) & (line != 0)].reset_index(name='line')
    else:
        line_df = line[~line.isna()].reset_index(name='line')
    encode_point_args = {
        'x': altair.X(
            field=date_col,
            type=x_type,
            axis=altair.Axis(title=x_title),
        ),
        'y': altair.Y(
            field='points',
            type='quantitative',
            aggregate='sum',
            axis=altair.Axis(title=''),
            scale=altair.Scale(
                type=scale
            ),
        ),
    }
    encode_line_args = {
        'x': altair.X(
            field=date_col,
            type=x_type
        ),
        'y': altair.Y(
            field='line',
            type='quantitative',
            aggregate='sum',
            scale=altair.Scale(
                type=scale
            ),
            axis=altair.Axis(title=y_title),
        ),
    }
    mark_point_args = {
        'opacity':0.7,
        'filled':True,
        'size':15,
    }
    mark_line_args = {
    }
    if colour in line_df.columns:
        if len(colour_domain) == 0:
            encode_point_args['color'] = colour
            encode_line_args['color'] = colour
        else:
            encode_point_args['color'] = altair.Color(
                field=colour,
                type='nominal',
                scale=altair.Scale(
                    domain=colour_domain,
                    range=colour_range
                )
            )
            encode_line_args['color'] = encode_point_args['color']
    else:
        mark_point_args['color'] = colour
        mark_line_args['color'] = colour
    charts = [
        altair.Chart(
            line_df
        ).mark_line(
            **mark_line_args
        ).encode(
            **encode_line_args
        ).properties(
            width=width,
            height=height
        ),
    ]
    if points is not None:
        if scale=='log':
            points_df = points[(~points.isna()) & (points != 0)].reset_index(name='points')
        else:
            points_df = points[~points.isna()].reset_index(name='points')
        charts.append(
            altair.Chart(
                points_df
            ).mark_point(
                **mark_point_args
            ).encode(
                **encode_point_args
            )
        )
    return altair.layer(*charts
    )

def plot_points_average_and_trend(configs, title, footer):
    return altair.concat(
        altair.vconcat(
            *[points_average_and_trend(
                **c
            ) for c in configs]
        ).resolve_scale(
            x='shared'
        ).properties(
            title=altair.TitleParams(
                footer,
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

# %%
plt = plot_points_average_and_trend(
    [
        {
            'points': None,
            'line': df[(df['Date'] > '2021-05-08')].set_index(['Date','Nation'])['Rolling cases per 100k'],
            'colour': 'Nation',
            'date_col': 'Date',
            'x_title': 'Specimen Date',
            'y_title': 'New cases per 100k',
            'scale': 'log',
            'colour_domain': ['England','Scotland','Wales','Northern Ireland'],
            'colour_range': ['grey','#005eb8','#D30731','#076543']
        },
    ],
    '%s COVID-19 %s (7-day average, log scale) reported on %s' %(
        'UK',
        'cases per 100k people',
        datetime.datetime.today().strftime('%A %-d %B %Y'),
    ),
    [
        'Cases data from PHE dashboard/API, mid-2020 populations from ONS',
        'https://twitter.com/ni_covid19_data'
    ]
)
plt.save('nations-cases-100k-log-%s.png'%datetime.datetime.now().date().strftime('%Y-%d-%m'))
plt

# %%
for scale in ['log','linear']:
    plt = plot_points_average_and_trend(
        [
            {
                'points': None,
                'line': df[(df['Date'] > '2021-05-08')].set_index(['Date','Nation'])['Rolling cases per 100k'],
                'colour': 'Nation',
                'date_col': 'Date',
                'x_title': 'Specimen Date',
                'y_title': 'New cases per 100k',
                'scale': scale,
                'colour_domain': ['England','Scotland','Wales','Northern Ireland'],
                'colour_range': ['grey','#005eb8','#D30731','#076543']
            },
        ],
        '%s COVID-19 %s (7-day average, %s scale) reported on %s' %(
            'UK',
            'cases per 100k people',
            scale,
            datetime.datetime.today().strftime('%A %-d %B %Y'),
        ),
        [
            'Cases data from PHE dashboard/API',
            'https://twitter.com/ni_covid19_data'
        ]
    )
    plt.save('nations-cases-100k-%s-%s.png'%(scale,datetime.datetime.now().date().strftime('%Y-%d-%m')))
    plt

# %%
plot_points_average_and_trend(
    [
        {
            'points': df[(df['Nation']=='Northern Ireland') & (df['Date'] > '2021-06-01')].set_index('Date')['newCasesBySpecimenDate'],
            'line': df[(df['Nation']=='Northern Ireland') & (df['Date'] > '2021-06-01')].set_index('Date')['New cases 7-day rolling mean'],
            'colour': '#076543',
            'date_col': 'Date',
            'x_title': 'Specimen Date',
            'y_title': 'New cases',
            'scale': 'linear',
            'height': 225
        },
        {
            'points': admissions[(admissions['Admission Date'] > '2021-06-01')].set_index('Admission Date')['Number of Admissions'],
            'line': admissions[(admissions['Admission Date'] > '2021-06-01')].set_index('Admission Date')['Number of Admissions 7-day rolling mean'],
            'colour': '#076543',
            'date_col': 'Admission Date',
            'x_title': 'Date',
            'y_title': 'Hospital admissions',
            'scale': 'linear',
            'height': 225
        },
    ],
    '%s COVID-19 %s (daily and 7-day mean) reported on %s' %(
        'Northern Ireland',
        'cases and admissions',
        datetime.datetime.today().strftime('%A %-d %B %Y'),
    ),
    [
        'Dots show daily reports, line is 7-day rolling average',
        'Cases, admissions and deaths data from DoH daily data',
        'https://twitter.com/ni_covid19_data'
    ],
)

# %%
def plot_key_ni_stats_date_range(df, admissions, deaths, start_date, end_date):
    return plot_points_average_and_trend(
        [
            {
                'points': df[(df['Nation']=='Northern Ireland') & (df['Date'] >= start_date) & (df['Date'] <= end_date)].set_index('Date')['newCasesBySpecimenDate'],
                'line': df[(df['Nation']=='Northern Ireland') & (df['Date'] >= start_date) & (df['Date'] <= end_date)].set_index('Date')['New cases 7-day rolling mean'],
                'colour': '#076543',
                'date_col': 'Date',
                'x_title': 'Specimen Date',
                'y_title': 'New cases',
                'scale': 'linear',
                'height': 225
            },
            {
                'points': admissions[(admissions['Admission Date'] >= start_date) & (admissions['Admission Date'] <= end_date)].set_index('Admission Date')['Number of Admissions'],
                'line': admissions[(admissions['Admission Date'] >= start_date) & (admissions['Admission Date'] <= end_date)].set_index('Admission Date')['Number of Admissions 7-day rolling mean'],
                'colour': '#076543',
                'date_col': 'Admission Date',
                'x_title': 'Date of Admission',
                'y_title': 'Hospital admissions',
                'scale': 'linear',
                'height': 225
            },
            {
                'points': deaths[(deaths['Date of Death'] >= start_date) & (deaths['Date of Death'] <= end_date)].set_index('Date of Death')['Number of Deaths'],
                'line': deaths[(deaths['Date of Death'] >= start_date) & (deaths['Date of Death'] <= end_date)].set_index('Date of Death')['Number of Deaths 7-day rolling mean'],
                'colour': '#076543',
                'date_col': 'Date of Death',
                'x_title': 'Date of Death',
                'y_title': 'Deaths within 28 days of positive test',
                'scale': 'linear',
                'height': 225
            },
        ],
        '%s COVID-19 %s (daily and 7-day mean) between %s and %s' %(
            'Northern Ireland',
            'cases and admissions',
            datetime.datetime.strptime(start_date, '%Y-%m-%d').strftime('%-d %B %Y'),
            datetime.datetime.strptime(end_date, '%Y-%m-%d').strftime('%-d %B %Y'),
        ),
        [
            'Dots show daily reports, line is 7-day rolling average',
            'Cases, admissions and deaths data from DoH daily data',
            'https://twitter.com/ni_covid19_data'
        ],
    )

# %% All data
# Note how the cases peak is behind the admissions peak as testing ramps up
# The deaths peak is around 21 days after the admissions peak
plot_key_ni_stats_date_range(df, admissions, deaths, '2020-03-18', '2021-07-23')

# %% First wave
# Note how the cases peak is behind the admissions peak as testing ramps up
# The deaths peak is around 21 days after the admissions peak
plot_key_ni_stats_date_range(df, admissions, deaths, '2020-03-18', '2020-05-31')

# %% Winter 2020 peak
# Admissions peak is 10 days after the cases peak
# Deaths peak is 9 days after the admissions peak
plot_key_ni_stats_date_range(df, admissions, deaths, '2020-12-01', '2021-03-01')

# %% Summer 2021 growth
plot_key_ni_stats_date_range(df, admissions, deaths, '2021-06-01', '2021-07-22')

# %%
waves = pandas.DataFrame()
for wave in [
#    {
#        'name': 'Spring 2020',
#        'start_date': '2020-02-23',
#        'end_date': '2020-08-28',
#    },
    {
        'name': 'Autumn 2020',
        'start_date': '2020-08-29',
        'end_date': '2021-06-10',
    },
    {
        'name': 'Summer 2021',
        'start_date': '2021-06-11',
        'end_date': '2021-09-30',
    },
]:
    wavedf = pandas.DataFrame(pandas.date_range(wave['start_date'], wave['end_date']), columns=['Date'])
    wavedf['Wave'] = wave['name']
    wavedf['Wave start'] = pandas.to_datetime(wave['start_date'])
    waves = pandas.concat([
        waves,
        wavedf
    ])
nidf = df.merge(waves, left_on='Date', right_on='Date')
nidf['Days from start'] = (nidf['Date'] - nidf['Wave start']).dt.days
hospdf = admissions.merge(waves, left_on='Admission Date', right_on='Date')
hospdf['Days from start'] = (hospdf['Admission Date'] - hospdf['Wave start']).dt.days
deathsdf = deaths.merge(waves, left_on='Date of Death', right_on='Date')
deathsdf['Days from start'] = (deathsdf['Date of Death'] - deathsdf['Wave start']).dt.days
trim_days = 50
plot_points_average_and_trend(
    [
        {
            'points': None, #nidf[(nidf['Nation']=='Northern Ireland') & (nidf['Days from start'] < trim_days)].set_index(['Days from start','Wave'])['newCasesBySpecimenDate'],
            'line': nidf[(nidf['Nation']=='Northern Ireland') & (nidf['Days from start'] < trim_days)].set_index(['Days from start','Wave'])['New cases 7-day rolling mean'],
            'colour': 'Wave',
            'date_col': 'Days from start',
            'x_title': 'Days from start of wave',
            'y_title': 'New cases',
            'scale': 'linear',
            'height': 225,
            'x_type': 'ordinal'
        },
        {
            'points': None, #hospdf[hospdf['Days from start'] < trim_days].set_index(['Wave','Days from start'])['Number of Admissions'],
            'line': hospdf[hospdf['Days from start'] < trim_days].set_index(['Wave','Days from start'])['Number of Admissions 7-day rolling mean'],
            'colour': 'Wave',
            'date_col': 'Days from start',
            'x_title': 'Days from start of wave',
            'y_title': 'Hospital admissions',
            'scale': 'linear',
            'height': 225,
            'x_type': 'ordinal'
        },
        {
            'points': None, #deathsdf[deathsdf['Days from start'] < trim_days].set_index(['Wave','Days from start'])['Number of Deaths'],
            'line': deathsdf[deathsdf['Days from start'] < trim_days].set_index(['Wave','Days from start'])['Number of Deaths 7-day rolling mean'],
            'colour': 'Wave',
            'date_col': 'Days from start',
            'x_title': 'Days from start of wave',
            'y_title': 'Deaths within 28 days of positive test',
            'scale': 'linear',
            'height': 225,
            'x_type': 'ordinal'
        },
    ],
    '%s COVID-19 %s (daily and 7-day mean) in each phase of the pandemic' %(
        'Northern Ireland',
        'cases, admissions and deaths',
    )
)

# %% Build dataframe for predicting admissions from cases
def predict_admissions_from_cases(df, admissions, case_col, adm_col, day_gap, days_to_use, train_start, train_end, test_start):
    pred_hosp_from_cases = df[(df['Nation']=='Northern Ireland')][['Date',case_col]].dropna()
    pred_hosp_from_cases['Date'] = pred_hosp_from_cases['Date'] + pandas.DateOffset(days=3)
    hosp = admissions[['Admission Date',adm_col]]
    hosp['Admission Date'] = hosp['Admission Date'] + pandas.DateOffset(days=3)
    hosp['Date'] = hosp['Admission Date'] - pandas.DateOffset(days=day_gap)
    hosp.rename(columns={adm_col: 'Actual'}, inplace=True)
    pred_hosp_from_cases = pred_hosp_from_cases.merge(hosp, how='left', left_on='Date', right_on='Date')
    for i in range(days_to_use):
        pred_hosp_from_cases[i] = pred_hosp_from_cases[case_col].shift(i)
    regr = LinearRegression()
    x_cols = [i for i in range(days_to_use)]
    train = pred_hosp_from_cases[(pred_hosp_from_cases['Date'] >= train_start) & ((pred_hosp_from_cases['Date'] < train_end))]
    test = pred_hosp_from_cases[(pred_hosp_from_cases['Date'] >= test_start)]
    regr.fit(train[x_cols], train['Actual'])
    train['Predicted'] = regr.predict(train[x_cols])
    test['Predicted'] = regr.predict(test[x_cols])
    test['Admission Date'] = test['Date'] + pandas.DateOffset(days=day_gap)
    test_plot = test.melt(id_vars='Admission Date', value_vars=['Actual','Predicted'])
    train_plot = train.melt(id_vars='Admission Date', value_vars=['Actual','Predicted'])
    print('Predicted admissions: %f' %test[~test['Actual'].isna()]['Predicted'].sum())
    print('Actual admissions: %f' %test['Actual'].sum())
    return altair.vconcat(
        altair.Chart(
            train_plot
        ).mark_line().encode(
            x='Admission Date:T',
            y=altair.Y('value',axis=altair.Axis(title='Number of Admissions')),
            color='variable'
        ),
        altair.Chart(
            test_plot
        ).mark_line().encode(
            x='Admission Date:T',
            y=altair.Y('value',axis=altair.Axis(title='Number of Admissions')),
            color='variable'
        )
    )

# %%
predict_admissions_from_cases(df, admissions, 'New cases 7-day rolling mean', 'Number of Admissions 7-day rolling mean', 10, 5, '2020-09-01', '2020-11-01', '2021-07-01')

# %%
predict_admissions_from_cases(df, admissions, 'New cases 7-day rolling mean', 'Number of Admissions 7-day rolling mean', 10, 5, '2021-06-01', '2021-07-01', '2021-07-01')

# %%
predict_admissions_from_cases(df, admissions, 'newCasesBySpecimenDate', 'Number of Admissions', 10, 5, '2021-06-01', '2021-07-01', '2021-07-01')

# %%
(test[~test['Number of Admissions 7-day rolling mean'].isna()]['Predicted'] - test[~test['Number of Admissions 7-day rolling mean'].isna()]['Number of Admissions 7-day rolling mean']).sum()

# %%
test['Number of Admissions 7-day rolling mean'].sum()

# %%
(test['Predicted'] / test['Number of Admissions 7-day rolling mean'])
