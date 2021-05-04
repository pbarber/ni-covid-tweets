# %% Imports
import pandas
import altair
import numpy

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

# %% Plot the change since Christmas for all UK nations
altair.Chart(
    df[df['Date']>'2020-12-24']
).mark_line().encode(
    x=altair.X(
        field='Date',
        type='temporal',
    ),
    y=altair.Y(
        field='New cases 7-day rolling mean',
        type='quantitative',
        scale=altair.Scale(
            type='log'
        ),
    ),
    color=altair.Color(
        field='Nation',
        type='nominal',
        scale=altair.Scale(
            domain=['England','Scotland','Wales', 'Northern Ireland'],
            range=['grey', '#005eb8', '#D30731', '#076543']
        )
    )
).properties(
    height=450,
    width=800
)

# %% Plot just NI since Christmas
altair.vconcat(
    *[
        altair.layer(
            covid_timeline_ticks(height=300),
            altair.Chart(
                df[(df['Date']>'2020-12-24') & (df['Nation']=='Northern Ireland')]
            ).mark_line(
                color='#076543',
            ).encode(
                x=altair.X(
                    field='Date',
                    type='temporal',
                    axis=None,
                ),
                y=altair.Y(
                    field='New cases 7-day rolling mean',
                    type='quantitative',
                    scale=altair.Scale(
                        type='log'
                    ),
                )
            ),
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

def plot_fit(nation, colour, totdays, fitdays, ignoredays=0):
    nofit_dates = df[(~df['New cases 7-day rolling mean'].isna()) & (df['Nation']==nation)]['Date'].drop_duplicates().nlargest(totdays+ignoredays).nsmallest(totdays-fitdays)
    fit_dates = df[(~df['New cases 7-day rolling mean'].isna()) & (df['Nation']==nation)]['Date'].drop_duplicates().nlargest(fitdays+ignoredays).nsmallest(fitdays)
    ignore_dates= df[(~df['New cases 7-day rolling mean'].isna()) & (df['Nation']==nation)]['Date'].drop_duplicates().nlargest(ignoredays)
    nofit = altair.Chart(
        df[((df['Date'].isin(nofit_dates)) | df['Date'].isin(ignore_dates)) & (~df['New cases 7-day rolling mean'].isna()) & (df['Nation']==nation)]
    ).mark_point(
        color=colour,
        opacity=0.7
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
    )

    tofit = df[(df['Date'].isin(fit_dates)) & (~df['New cases 7-day rolling mean'].isna()) & (df['Nation']==nation)]
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
            field='New cases 7-day rolling mean',
            type='quantitative',
            aggregate='sum',
            scale=altair.Scale(
                type='log'
            ),
            title='New cases (7-day rolling mean)'
        )
    )

    tofit['x'] = (tofit['Date']  - df['Date'].min()).dt.days
    curve = numpy.polyfit(tofit['x'], numpy.log(tofit['New cases 7-day rolling mean']), 1)
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
    print(curve)

    lobf = altair.Chart(
        tofit
    ).mark_line(
        color=colour,
        opacity=0.7
    ).encode(
        x=altair.X(
            field='Date',
            type='temporal'
        ),
        y=altair.Y(
            field='result',
            type='quantitative',
            aggregate='sum',
            scale=altair.Scale(
                type='log'
            ),
            title='New cases (7-day rolling mean)'
        )
    )

    labels = altair.Chart(
        model
    ).transform_calculate(
        pct=f'"Daily " + datum.RF + ": " + format(datum.Daily,".1%")',
        pct_wk=f'"Weekly " + datum.RF + ": " + format(datum.Weekly,".1%")',
        hd=f'datum.HD + " time: " + format(datum.HD_time,".1f") + " days"',
        date=f'"Model: " + datum.Date'
    )

    return altair.layer(
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
    )

# %%
plot_fit('Northern Ireland', '#076543', 42, 9, 1)

# %%
plot_fit('Wales', '#D30731', 42, 9, 1)

# %%
plot_fit('Scotland', '#005eb8', 42, 9, 1)

# %%
plot_fit('England', 'grey', 42, 9, 1)

# %%


