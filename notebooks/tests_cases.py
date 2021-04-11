# %% imports
import pandas
import altair

altair.data_transformers.disable_max_rows()

# %% Load the data from the aggregated CSV
df = pandas.read_csv('../data/all_tests.csv', index_col=None)
df['Date of Specimen'] = pandas.to_datetime(df['Date of Specimen'], format='%Y-%m-%d')
df['Reported_Date'] = pandas.to_datetime(df['Reported_Date'], format='%Y-%m-%d')
df['Days since test'] = (df['Reported_Date']-df['Date of Specimen']).dt.days
df['Individ with Lab Test relative'] = df.sort_values('Reported_Date').groupby('Date of Specimen')['Individ with Lab Test'].apply(lambda x: x.div(x.iloc[0]))
df['Day of Specimen'] = df['Date of Specimen'].dt.day_name()
df['Week of Specimen'] = df['Date of Specimen'].dt.isocalendar().week

# %% Get the latest report
latest = df[df['Reported_Date']==df['Reported_Date'].max()]
latest['Metric'] = 'Latest download'
latest.drop(columns=['Reported_Date'], inplace=True)
latest.rename(columns={'Date of Specimen':'Date'}, inplace=True)
latest['Total Lab Tests Rolling'] = latest['Total Lab Tests'].rolling(7).mean()
latest['Individ with Lab Test Rolling'] = latest['Individ with Lab Test'].rolling(7).mean()
latest['Individ with Positive Lab Test Rolling'] = latest['Individ with Positive Lab Test'].rolling(7).mean()

# %% Do the dashboard's new individuals calculation
new_individuals = df.groupby(['Reported_Date']).sum().sort_index().diff()
new_individuals['Metric'] = 'Newly reported'
new_individuals.reset_index(inplace=True)
new_individuals.rename(columns={'Reported_Date':'Date'}, inplace=True)
new_individuals['Total Lab Tests Rolling'] = new_individuals['Total Lab Tests'].rolling(7).mean()
new_individuals['Individ with Lab Test Rolling'] = new_individuals['Individ with Lab Test'].rolling(7).mean()
new_individuals['Individ with Positive Lab Test Rolling'] = new_individuals['Individ with Positive Lab Test'].rolling(7).mean()

# %% Get the last 24 hour reported data for each report
daily_24_hours = df.sort_values('Date of Specimen').groupby(['Reported_Date']).last()
daily_24_hours['Metric'] = 'Last 24 hours'
daily_24_hours.drop(columns=['Date of Specimen'], inplace=True)
daily_24_hours.reset_index(inplace=True)
daily_24_hours.rename(columns={'Reported_Date':'Date'}, inplace=True)
daily_24_hours['Total Lab Tests Rolling'] = daily_24_hours['Total Lab Tests'].rolling(7).mean()
daily_24_hours['Individ with Lab Test Rolling'] = daily_24_hours['Individ with Lab Test'].rolling(7).mean()
daily_24_hours['Individ with Positive Lab Test Rolling'] = daily_24_hours['Individ with Positive Lab Test'].rolling(7).mean()

# %% Join the three dataframes to give a view of the different metrics
combined = pandas.concat([daily_24_hours, new_individuals, latest])
combined['Positivity Rate Rolling'] = combined['Individ with Positive Lab Test Rolling']/combined['Individ with Lab Test Rolling']

# %% Plot total lab tests (rolling) for all three metrics
altair.Chart(
    combined[combined['Date']>'2020-07-31']
).mark_line().encode(
    x=altair.X(r'Date:T'),
    y=altair.Y(r'Total\ Lab\ Tests\ Rolling:Q', axis=altair.Axis(format=',d', title='Total Lab Tests (7-day rolling mean)')),
    color='Metric'
)

# %% Plot individuals with lab tests (rolling) for all three metrics
altair.Chart(
    combined[combined['Date']>'2020-07-31']
).mark_line().encode(
    x=altair.X(r'Date:T'),
    y=altair.Y(r'Individ\ with\ Lab\ Test\ Rolling:Q', axis=altair.Axis(format=',d', title='Individuals with Lab Tests (7-day rolling mean)')),
    color='Metric'
)

# %% Plot individuals with positive lab tests (rolling) for all three metrics
altair.Chart(
    combined[combined['Date']>'2020-07-31']
).mark_line().encode(
    x=altair.X(field='Date', type='temporal'),
    y=altair.Y(field='Individ with Positive Lab Test Rolling', type='quantitative', axis=altair.Axis(format=',d', title='Individuals with Positive Lab Tests (7-day rolling mean)')),
    color='Metric'
)

# %% Plot PR (rolling) for the two sensible metrics
altair.Chart(
    combined[(combined['Date']>'2020-07-31') & (combined['Metric'].isin(['Last 24 hours','Latest download']))]
).mark_line().encode(
    x=altair.X(r'Date:T'),
    y=altair.Y(r'Positivity\ Rate\ Rolling:Q', axis=altair.Axis(format='%', title='Percentage individuals positive (7-day rolling mean)')),
    color='Metric'
)

# %% Plot 1st Feb tests by report
altair.Chart(
    df[df['Date of Specimen']=='2021-02-01']
).mark_line().encode(
    x=altair.X(r'Reported_Date:T'),
    y=altair.Y(r'Individ\ with\ Lab\ Test:Q', axis=altair.Axis(format=',d', title='Individuals with Lab Tests (1st Feb 2021)'))
)

# %% Plot 1st Feb cases by report
altair.Chart(
    df[df['Date of Specimen']=='2021-02-01']
).mark_line().encode(
    x=altair.X(r'Reported_Date:T'),
    y=altair.Y(r'Individ\ with\ Positive\ Lab\ Test:Q', axis=altair.Axis(format=',d', title='Individuals with Lab Tests (1st Feb 2021)'))
)

# %% Plot 1st March tests by days since test
altair.Chart(
    df[df['Date of Specimen']=='2021-03-01']
).mark_line().encode(
    x=altair.X(r'Days\ since\ test:N', axis=altair.Axis(title='Days since specimen date')),
    y=altair.Y(r'Individ\ with\ Lab\ Test:Q', axis=altair.Axis(format=',d', title='Individuals with Lab Tests (1st Mar 2021)'))
)

# %% Plot all tests by days since test
altair.Chart(
    df[(df['Date of Specimen'] > '2020-09-30') & (df['Days since test'] < 35) & (df['Week of Specimen'].isin([44,45,46]))]
).mark_line().encode(
    x=altair.X(r'Days\ since\ test:N', axis=altair.Axis(title='Days since specimen date')),
    y=altair.Y(r'Individ\ with\ Lab\ Test:Q', axis=altair.Axis(format=',d', title='Individuals with lab test')),
    color=altair.Color(r'Day\ of\ Specimen:N', sort=['Monday','Tuesday','Wednesday','Thursday','Friday','Saturday','Sunday'], title='Weekday')
).facet(
    row=r'Week\ of\ Specimen:O'
).resolve_scale(
    y="independent"
)

# %% Plot weekly totals
altair.Chart(
    df[(df['Date of Specimen'] > '2020-09-30') & (df['Days since test'] < 35) & (df['Week of Specimen'].isin([2,3,4,5,6,7,8]))]
).mark_line().encode(
    x=altair.X(field='Days since test', type='ordinal', axis=altair.Axis(title='Days since specimen date')),
    y=altair.Y(field='Individ with Lab Test', type='quantitative', aggregate='sum', axis=altair.Axis(format=',d', title='Individuals with lab test')),
    color=altair.Color(field='Week of Specimen', type='ordinal')
)

# %% A more accurate metric that remains consistent - sum most recent N days, subtract previous report of those days

# For each reported day, sum where days since test < N, get the preceding reported day for the summed
# dates and calculate the difference

