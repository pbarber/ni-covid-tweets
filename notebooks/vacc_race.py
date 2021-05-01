# %% Imports
import pandas
import bar_chart_race as bcr
from matplotlib import colors

# %% Load data from PHE API and transform it to the right format
df = pandas.read_csv('https://api.coronavirus.data.gov.uk/v2/data?areaType=nation&metric=cumVaccinationFirstDoseUptakeByPublishDatePercentage&metric=cumVaccinationSecondDoseUptakeByPublishDatePercentage&format=csv')
df.drop(columns=['areaType','areaCode'], inplace=True)
df.rename(columns={'cumVaccinationFirstDoseUptakeByPublishDatePercentage':' 1st dose','cumVaccinationSecondDoseUptakeByPublishDatePercentage':' 2nd dose'},inplace=True)
df = df.melt(id_vars=['date','areaName'], var_name='dose')
df['metric'] = df['areaName'] + df['dose']
df.drop(columns=['areaName','dose'],inplace=True)
df = df.pivot(index='date', columns='metric', values='value')
# Duplicate last row so it displays
df = pandas.concat([df,pandas.DataFrame(df[-1:].values, index=[df.index.max()], columns=df.columns)])

# %% Build the bar chart race, with suitable country colours
# %%
from matplotlib import rcParams
rcParams['font.family'] = 'sans-serif'
cmap = colors.ListedColormap(['white', 'white', '#076543', '#076543', '#005eb8', '#005eb8', '#D30731', '#D30731'])
bcr.bar_chart_race(
    df,
    title=r'COVID-19 vaccination progress (% of adults) in the UK',
    filename=None,
    steps_per_period=10,
    figsize=(16,9),
    dpi=240,
    title_size=28,
    bar_label_size=24,
    tick_label_size=24,
    period_label={'x': .99, 'y': .25, 'ha': 'right', 'va': 'center', 'size': 22},
    cmap=cmap)

