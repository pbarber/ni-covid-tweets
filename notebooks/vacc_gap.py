# %%
import pandas
import altair
import datetime

# %%
df = pandas.read_csv('https://api.coronavirus.data.gov.uk/v2/data?areaType=nation&metric=cumPeopleVaccinatedFirstDoseByPublishDate&metric=cumPeopleVaccinatedThirdInjectionByPublishDate&metric=cumPeopleVaccinatedSecondDoseByPublishDate&format=csv')
df.rename(columns={
    'cumPeopleVaccinatedFirstDoseByPublishDate': 'First',
    'cumPeopleVaccinatedSecondDoseByPublishDate': 'Second',
    'cumPeopleVaccinatedThirdInjectionByPublishDate': 'Third/Booster',
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
    dfa['Gap2'] = (dfa.index - dfa['Third/Booster'].apply(latest_where_lte, col=dfa['Second'])).dt.days
    out = pandas.concat([out, dfa.reset_index()])


# %%
latest = out[out['Publication Date'] == out['Publication Date'].max()]
trend = altair.Chart(
    out[(~out['Gap'].isna())]
).mark_line().encode(
    x = altair.X(
        'Publication Date:T',
        title='Publication Date of Second Dose'
    ),
    y = altair.Y(
        'Gap:Q',
        title='First to Second Dose Gap (days)',
        scale=altair.Scale(zero=False)
    ),
    color=altair.Color(
        'Nation',
        legend=None,
        type='nominal',
        scale=altair.Scale(
            domain=['England','Scotland','Wales','Northern Ireland'],
            range=['grey','#005eb8','#D30731','#076543']
        )
    )
)
text = altair.Chart(latest).mark_text(
    align='left',
    baseline='middle',
    dx=5
).encode(
    x = 'Publication Date',
    y = altair.Y(
        field='Gap',
        type='quantitative',
        aggregate='sum',
    ),
    color=altair.Color(
        'Nation',
        type='nominal',
        scale=altair.Scale(
            domain=['England','Scotland','Wales','Northern Ireland'],
            range=['grey','#005eb8','#D30731','#076543']
        )
    ),
    text = altair.Text('Nation')
)
p = altair.concat(
    altair.layer(
        trend,
        text
    ).properties(
        height=450,
        width=800,
        title=altair.TitleParams(
            ['Data from https://coronavirus.data.gov.uk',
            'https://twitter.com/ni_covid19_data on %s' %datetime.datetime.now().strftime('%A %-d %B %Y')
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
        'Gap in days between total first and second dose vaccinations for UK nations',
        anchor='middle',
    )
)
p.save('uk-second-gap-%s.png'%(datetime.datetime.now().date().strftime('%Y-%m-%d')))
p

# %%
trend = altair.Chart(
    out[(~out['Gap2'].isna()) & (out['Publication Date'] > '2021-10-12')]
).mark_line().encode(
    x = altair.X(
        'Publication Date:T',
        title='Publication Date of Third/Booster'
    ),
    y = altair.Y(
        'Gap2:Q',
        title='Second to Third/Booster Dose Gap (days)',
        scale=altair.Scale(zero=False)
    ),
    color=altair.Color(
        'Nation',
        legend=None,
        type='nominal',
        scale=altair.Scale(
            domain=['England','Scotland','Wales','Northern Ireland'],
            range=['grey','#005eb8','#D30731','#076543']
        )
    )
)
text = altair.Chart(latest).mark_text(
    align='left',
    baseline='middle',
    dx=5
).encode(
    x = 'Publication Date',
    y = altair.Y(
        field='Gap2',
        type='quantitative',
        aggregate='sum',
    ),
    color=altair.Color(
        'Nation',
        type='nominal',
        scale=altair.Scale(
            domain=['England','Scotland','Wales','Northern Ireland'],
            range=['grey','#005eb8','#D30731','#076543']
        )
    ),
    text = altair.Text('Nation')
)
p = altair.concat(
    altair.layer(
        trend,
        text
    ).properties(
        height=450,
        width=800,
        title=altair.TitleParams(
            ['Data from https://coronavirus.data.gov.uk',
            'https://twitter.com/ni_covid19_data on %s' %datetime.datetime.now().strftime('%A %-d %B %Y')
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
        'Gap in days between total second and third/booster dose vaccinations for UK nations',
        anchor='middle',
    )
)
p.save('uk-booster-gap-%s.png'%(datetime.datetime.now().date().strftime('%Y-%m-%d')))
p


# %%
latest
# %%
