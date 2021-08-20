

import pandas
import requests

def get_ons_pop_pyramid(url):
    resp = requests.get(url)
    resp.raise_for_status()
    data = resp.json()['ons']
    series = pandas.Series(data['series'], name='Gender')
    value = pandas.DataFrame(data['value'])
    time = pandas.DataFrame(data['time'])
    dimension = pandas.DataFrame(data['dimension'])
    value.columns = dimension['index']
    value = value.merge(series, left_index=True, right_index=True)
    value = value.melt(var_name='Age Band', id_vars='Gender')
    value.set_index(['Gender','Age Band'],inplace=True)
    df = pandas.DataFrame(value['value'].tolist(), index=value.index)
    df.columns = time['index'].values
    df = df.reset_index().melt(id_vars=['Gender','Age Band'], var_name='Year')
    df.set_index(['Gender','Age Band','Year'],inplace=True)
    df = pandas.DataFrame(df['value'].tolist(), index=df.index)
    df.columns = ['Population','% of population']
    df.reset_index(inplace=True)
    df['Year'] = df['Year'].astype(int)
    df['Age Band'] = df['Age Band'].astype(int)
    return df

def get_uk_pop_pyramid():
    return get_ons_pop_pyramid('https://www.ons.gov.uk/visualisations/dvc671/pyramids2/pyramids/data/unitedkingdom.json')

def get_eng_pop_pyramid():
    return get_ons_pop_pyramid('https://www.ons.gov.uk/visualisations/dvc1430/pyramids/pyramids/data/E92000001.json')

def get_ni_pop_pyramid():
    return get_ons_pop_pyramid('https://www.ons.gov.uk/visualisations/dvc1430/pyramids/pyramids/data/N92000002.json')