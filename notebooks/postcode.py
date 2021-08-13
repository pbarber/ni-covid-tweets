# %%
import pandas
import altair

# %% Load latest postcode cases data
dd = pandas.read_excel('~/Downloads/doh-dd-120821.xlsx', sheet_name='Tests by Postal District-7 days')
vacc = pandas.read_csv('postcodes.csv')
df = vacc.merge(dd, how='inner', left_on='Postcode District', right_on='Postal_District')
df = df[['Postcode District','Vaccinations','Rate per 100K Pop.','Population_y']].rename(columns={'Rate per 100K Pop.': 'Cases per 100k People'})
df['Vaccinations per Person'] = df['Vaccinations']/df['Population_y']

# %%
plt = altair.Chart(df).mark_point().encode(
    x=altair.X('Vaccinations per Person:Q'),
    y=altair.Y('Cases per 100k People:Q'),
    tooltip='Postcode District:N'
)
plt.show()
# %%
vacc['Population'].sum()
# %%
vacc['Vaccinations'].sum()
