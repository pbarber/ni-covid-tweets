import io

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

def get_s3_csv_or_empty_df(s3, bucketname, keyname, columns):
    try:
        obj = s3.get_object(Bucket=bucketname,Key=keyname)['Body']
    except s3.exceptions.NoSuchKey:
        print("The object %s does not exist in bucket %s." %(keyname, bucketname))
        return pandas.DataFrame(columns=columns)
    else:
        stream = io.BytesIO(obj.read())
        return pandas.read_csv(stream)

def push_csv_to_s3(df, s3, bucketname, keyname):
    # Push the data to s3
    stream = io.BytesIO()
    df.to_csv(stream, index=False)
    stream.seek(0)
    s3.upload_fileobj(stream, bucketname, keyname)

def update_datastore(s3, bucketname, keyname, last_updated, df, store, datecol='Date'):
    # Pull current data from s3, or empty dataframe
    datastore = get_s3_csv_or_empty_df(s3, bucketname, keyname, [datecol])
    # Clean out any data with matching dates
    datastore = datastore[pandas.to_datetime(datastore[datecol]).dt.date != last_updated.date()]
    # Append the new data
    datastore = pandas.concat([datastore, df])
    datastore[datecol] = datastore[datecol].fillna(last_updated)
    datastore[datecol] = pandas.to_datetime(datastore[datecol])
    # Push the data to s3
    if store is True:
        push_csv_to_s3(datastore, s3, bucketname, keyname)
    return datastore
