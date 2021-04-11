# %% Imports
import pandas
import bar_chart_race as bcr
from matplotlib import colors

# %% Load data from PHE API
df = pandas.read_csv('https://api.coronavirus.data.gov.uk/v2/data?areaType=nation&metric=cumVaccinationFirstDoseUptakeByPublishDatePercentage&metric=cumVaccinationSecondDoseUptakeByPublishDatePercentage&format=csv')

# %% Get the data in the right format
df.drop(columns=['areaType','areaCode'], inplace=True)
df.rename(columns={'cumVaccinationFirstDoseUptakeByPublishDatePercentage':' 1st dose','cumVaccinationSecondDoseUptakeByPublishDatePercentage':' 2nd dose'},inplace=True)
df = df.melt(id_vars=['date','areaName'], var_name='dose')
df['metric'] = df['areaName'] + df['dose']
df.drop(columns=['areaName','dose'],inplace=True)
df = df.pivot(index='date', columns='metric', values='value')

# %% Build the bar chart race, with suitable country colours
cmap = colors.ListedColormap(['white', 'white', '#076543', '#076543', '#005eb8', '#005eb8', '#D30731', '#D30731'])
bcr.bar_chart_race(
    df,
    title=r'COVID-19 vaccination progress (% of adults) in the UK',
    filename=None,
    steps_per_period=5,
    shared_fontdict={'family': 'Source Code Pro'},
    dpi=600,
    cmap=cmap)

# %%
