# %%
import pandas

# %% Model 0 - back of envelope
key_stats = pandas.DataFrame(
    {
        'group': ['16+'],
        'antibodies': [0.904], # Taken from https://www.health-ni.gov.uk/sites/default/files/publications/health/covid-19-antibodies-01-09-21.pdf
        'antibodies_lower': [0.867],
        'antibodies_upper': [0.929],
        'infected_upper': [38400], # https://www.health-ni.gov.uk/sites/default/files/publications/health/covid-19-infection-survey-03-09-21.pdf
        'infected_lower': [20300],
        'deaths': [53], # w/e 27th August
        'cases': [11107], # w/e 29th August from https://www.health-ni.gov.uk/sites/default/files/publications/health/doh-db-310821.pdf,
        'population': 1499694   # NISRA mid-2020 estimate
    }
)
key_stats['CFR'] = key_stats['deaths']/key_stats['cases']
print('Upper estimate for cases %s' % ((key_stats['population'] * (1-key_stats['antibodies_lower'])) * (key_stats['cases']/key_stats['infected_lower'])).sum())
print('Lower estimate for cases %s' % ((key_stats['population'] * (1-key_stats['antibodies_upper'])) * (key_stats['cases']/key_stats['infected_upper'])).sum())
print('Upper estimate for deaths %s' % ((key_stats['population'] * (1-key_stats['antibodies_lower'])) * (key_stats['cases']/key_stats['infected_lower']) * key_stats['CFR']).sum())
print('Lower estimate for deaths %s' % ((key_stats['population'] * (1-key_stats['antibodies_upper'])) * (key_stats['cases']/key_stats['infected_upper']) * key_stats['CFR']).sum())

# %% Model 1 - SIR model for dummies
def sir_for_dummies(start_date, end_date, susceptible, infected, growth_rate, vacc_per_week=0):
    df = pandas.DataFrame(columns=['Date'])
    for date in pandas.date_range(pandas.to_datetime(start_date),pandas.to_datetime(end_date),freq=pandas.DateOffset(7)).date:
        df = df.append(
            {
                'Date': date,
                'Susceptible': susceptible,
                'Infected': infected
            },
            ignore_index=True
        )
        if susceptible > 0:
            case_rate = (infected / susceptible)
        else:
            case_rate = 0
        susceptible = susceptible - infected - vacc_per_week
        if susceptible < 0:
            susceptible = 0
        infected = susceptible * case_rate * growth_rate
    return df

# %%
# Fixed level of infection, no vaccinations
susceptible_u16 = ((1-0.25) * 395816)
susceptible_o16 = ((1-0.929) * 1499694)
df = sir_for_dummies('2021-08-28', '2021-12-25', susceptible_o16 + susceptible_u16, 20300, 1.3)
print('Upper estimate for cases %s' % int((df['Infected'] * (11107/20300)).sum()))
print('Upper estimate for deaths %s' % int((df['Infected'] * (11107/38400) * (susceptible_o16/(susceptible_o16+susceptible_u16)) * (53/11107)).sum()))
susceptible_u16 = ((1-0.2) * 395816)
susceptible_o16 = ((1-0.867) * 1499694)
df = sir_for_dummies('2021-08-28', '2021-12-25', susceptible_o16 + susceptible_u16, 38400, 1.3)
print('Lower estimate for cases %s' % int((df['Infected'] * (11107/38400)).sum()))
print('Lower estimate for deaths %s' % int((df['Infected'] * (11107/38400) * (susceptible_o16/(susceptible_o16+susceptible_u16)) * (53/11107)).sum()))

# %%
# Fixed level of infection, 4000 vaccinations per week
susceptible_u16 = ((1-0.25) * 395816)
susceptible_o16 = ((1-0.929) * 1499694)
df = sir_for_dummies('2021-08-28', '2021-12-25', susceptible_o16 + susceptible_u16, 20300, 0.9, 4000)
print('Upper estimate for cases %s' % int((df['Infected'] * (11107/20300)).sum()))
print('Upper estimate for deaths %s' % int((df['Infected'] * (11107/20300) * (susceptible_o16/(susceptible_o16+susceptible_u16)) * (53/11107)).sum()))
susceptible_u16 = ((1-0.2) * 395816)
susceptible_o16 = ((1-0.867) * 1499694)
df = sir_for_dummies('2021-08-28', '2021-12-25', susceptible_o16 + susceptible_u16, 38400, 0.9, 4000)
print('Lower estimate for cases %s' % int((df['Infected'] * (11107/38400)).sum()))
print('Upper estimate for deaths %s' % int((df['Infected'] * (11107/38400) * (susceptible_o16/(susceptible_o16+susceptible_u16)) * (53/11107)).sum()))
