# %% Imports
import pandas
import altair
import numpy
import datetime

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

# %% 2019 populations from Fig 3 of https://www.ons.gov.uk/peoplepopulationandcommunity/populationandmigration/populationestimates/bulletins/annualmidyearpopulationestimates/mid2019estimates
nationpop = pandas.DataFrame([
    {'Nation': 'Northern Ireland', 'Population': 1893667},
    {'Nation': 'England', 'Population': 56286961},
    {'Nation': 'Scotland', 'Population': 5463300},
    {'Nation': 'Wales', 'Population': 3152879},
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
df = create_models(df, 'Nation', 'Rolling cases per 100k')
df = create_models(df, 'Nation', 'newCasesBySpecimenDate')

# %% Load NI regional data
ni = pandas.read_excel('~/Downloads/doh-dd-110521.xlsx', sheet_name='Tests')
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
    domain=['England','Scotland','Wales','Northern Ireland'],
    range=['grey','#005eb8','#D30731','#076543']
)

# %% Plot the change in cases per 100k since Christmas for all UK nations
plot_multiple_trendlines(
    df[df['Date']>'2020-12-24'],
    y='Rolling cases per 100k',
    color='Nation',
    y_scale='log',
    domain=['England','Scotland','Wales','Northern Ireland'],
    range=['grey','#005eb8','#D30731','#076543']
)

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

# %% Functions to plot the last n days with trend line from last m days
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

# %%
def plot_points_average_and_trend(df, colour):
    df1 = df[(~df['newCasesBySpecimenDate'].isna()) & (df['newCasesBySpecimenDate'] != 0)]
    df2 = df[(~df['New cases 7-day rolling mean'].isna()) & (df['New cases 7-day rolling mean'] != 0)]
    return altair.layer(
        altair.Chart(
            df1
        ).mark_point(
            color=colour,
            opacity=0.7,
            filled=True,
            size=15,
        ).encode(
            x=altair.X(
                field='Date',
                type='temporal'
            ),
            y=altair.Y(
                field='newCasesBySpecimenDate',
                type='quantitative',
                aggregate='sum',
                scale=altair.Scale(
                    type='log'
                ),
            )
        ),
        altair.Chart(
            df2
        ).mark_line(
            color=colour
        ).encode(
            x=altair.X(
                field='Date',
                type='temporal'
            ),
            y=altair.Y(
                field='New cases 7-day rolling mean',
                type='quantitative',
                aggregate='sum',
                scale=altair.Scale(
                    type='log'
                ),
            )
        ),
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
plot_points_average_and_trend(df[(df['Nation']=='England') & (df['Date'] > '2021-04-23')],'grey').properties(
    height=450,
    width=800
)
