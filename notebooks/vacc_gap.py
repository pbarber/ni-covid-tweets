# %%
import pandas
import altair

# %%
df = pandas.read_csv('https://api.coronavirus.data.gov.uk/v2/data?areaType=nation&metric=cumPeopleVaccinatedFirstDoseByPublishDate&metric=cumPeopleVaccinatedSecondDoseByPublishDate&format=csv')
df.rename(columns={
    'cumPeopleVaccinatedFirstDoseByPublishDate': 'First',
    'cumPeopleVaccinatedSecondDoseByPublishDate': 'Second',
    'areaName': 'Nation',
    'date': 'Publication Date'
}, inplace=True)
df = df.drop(columns=['areaCode','areaType']).melt(id_vars=['Publication Date','Nation'], var_name='Dose', value_name='People')

# %%
altair.Chart(df).mark_line().encode(
    x = 'Publication Date:T',
    y = 'People:Q',
    color='Dose',
    facet=altair.Facet('Nation', columns=2)
).resolve_scale(
    y='independent'
)

# %%
def latest_where_lte(x, col):
    filtered = col[(col <= x)]
    if len(filtered) > 0:
        return filtered.idxmax()
    else:
        return pandas.NaT

out = pandas.DataFrame()
for nation in ['Northern Ireland','England','Wales','Scotland']:
    dfa = df[df['Nation']==nation].pivot(index=['Nation','Publication Date'], columns='Dose', values='People').reset_index().rename_axis(None, axis=1)
    dfa['Publication Date'] = pandas.to_datetime(dfa['Publication Date'])
    dfa.set_index(['Publication Date'], inplace=True)
    dfa['Gap'] = (dfa.index - dfa['Second'].apply(latest_where_lte, col=dfa['First'])).dt.days
    out = pandas.concat([out, dfa.reset_index()])


# %%
altair.Chart(out[~out['Gap'].isna()]).mark_line().encode(
    x = 'Publication Date:T',
    y = altair.Y('Gap:Q',title='First/Second Dose Gap (days)'),
    color='Nation'
)

# %%
altair.Chart(df[df['Publication Date'] > '2021-06-01']).mark_line().encode(
    x = 'Publication Date:T',
    y = 'People:Q',
    color='Dose',
    facet=altair.Facet('Nation', columns=2)
).resolve_scale(
    y='independent'
)
