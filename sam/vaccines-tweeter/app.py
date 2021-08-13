import json
import datetime
import logging
import time
import random
import io

import boto3
import requests
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.keys import Keys
import pandas
import numpy
import altair

from shared import S3_scraper_index, get_url
from twitter_shared import TwitterAPI
from plot_shared import get_chrome_driver

good_symb = '\u2193'
bad_symb = '\u2191'

green_block = '\u2705'
white_block = '\u2b1c'
black_block = '\u2b1b'

class GroupMatch:
    def __init__(self, left, right, len):
        self.left = left
        self.right = right
        self.len = len

def build_match_matrix(left, right):
    matches = [numpy.char.equal(l,right) for l in left]
    return numpy.vstack(matches)

def find_match_groups(matches):
    match_groups = []
    for i in range(-len(matches)+1,len(matches[0])):
        diag = numpy.diag(matches,i)
        if i < 0:
            a = -i
            b = 0
        else:
            a = 0
            b = i
        indices = diag.nonzero()[0]
        if len(indices) > 0:
            start = indices[0]
            match_groups.append(GroupMatch(a+indices[0],b+indices[0],1))
        for j in range(1,len(indices)):
            if indices[j] == start+1:
                match_groups[-1].len += 1
            else:
                match_groups.append(GroupMatch(a+indices[j],b+indices[j],1))
            start = indices[j]
    return match_groups

def get_new_items(old,new):
    matches = build_match_matrix(old,new)
    groups = find_match_groups(matches)
    max_len = 0
    for g in groups:
        if g.len > max_len:
            max_len = g.len
    found = [g for g in groups if g.len == max_len]
    nmax = len(found)
    if nmax > 1:
        raise Exception('%d matching sections found' %nmax)
    found = found[0]
    if found.right != 0:
        raise Exception('New section match started at non-zero location %d' %found.right)
    if (found.left + found.len) != len(old):
        raise Exception('Old section match finished before end %d' %(found.len + found.left))
    return new[(found.right+found.len):]

nisra_pops = pandas.DataFrame({
    'Postcode District': [
        'BT1','BT2','BT3','BT4','BT5','BT6','BT7','BT8','BT9','BT10','BT11','BT12','BT13','BT14','BT15','BT16','BT17','BT18','BT19','BT20','BT21','BT22','BT23','BT24','BT25','BT26','BT27','BT28','BT29','BT30','BT31','BT32','BT33','BT34','BT35','BT36','BT37','BT38','BT39','BT40','BT41','BT42','BT43','BT44','BT45','BT46','BT47','BT48','BT49','BT51','BT52','BT53','BT54','BT55','BT56','BT57','BT60','BT61','BT62','BT63','BT64','BT65','BT66','BT67','BT68','BT69','BT70','BT71','BT74','BT75','BT76','BT77','BT78','BT79','BT80','BT81','BT82','BT92','BT93','BT94',
    ],
    'Population': [
        2410,1050,850,25970,39510,31220,21150,30010,28580,12700,27430,29090,24200,33550,26440,17410,34070,13600,38320,25830,7760,22540,55720,18820,15760,9040,22480,44750,14520,34700,9430,26460,10990,63080,47930,47080,26350,41190,24050,26520,42380,36310,23200,25210,37130,11440,61460,60730,23680,26540,17740,25880,8050,8210,6820,4170,32520,16350,33930,21800,2430,8140,35610,24240,990,2590,21700,43160,17540,3700,2030,1600,30430,27610,24510,7860,26800,17190,11280,16150,
    ],
    'Population over 20': [
        0,0,0,20060,30290,24170,16840,22820,22560,9750,20610,20980,17770,23780,19950,13400,23630,10480,29100,20440,6190,17060,42800,14080,11440,7000,17270,33030,10140,25620,6740,19610,8320,45750,33420,35050,19920,31650,18150,20530,31040,27560,17400,18660,26680,8170,44990,43690,17850,19860,13280,19350,6090,6690,5330,3310,22950,12230,24730,16200,0,5530,26340,17730,0,1890,15420,30600,13090,2750,0,0,22290,20070,17930,5900,19560,12390,8490,11880,
    ],
    'Council Area': [
        'Belfast','Belfast','Belfast','Belfast','Lisburn and Castlereagh','Lisburn and Castlereagh','Belfast','Lisburn and Castlereagh','Belfast','Belfast','Belfast','Belfast','Belfast','Belfast','Belfast','Lisburn and Castlereagh','Lisburn and Castlereagh','Lisburn and Castlereagh','Ards and North Down','Ards and North Down','Ards and North Down','Ards and North Down','Newry, Mourne and Down','Newry, Mourne and Down','Newry, Mourne and Down','Lisburn and Castlereagh','Newry, Mourne and Down','Lisburn and Castlereagh','Lisburn and Castlereagh','Newry, Mourne and Down','Newry, Mourne and Down','Newry, Mourne and Down','Newry, Mourne and Down','Newry, Mourne and Down','Newry, Mourne and Down','Mid and East Antrim','Antrim and Newtownabbey','Mid and East Antrim','Mid and East Antrim','Mid and East Antrim','Mid and East Antrim','Mid and East Antrim','Mid and East Antrim','Mid and East Antrim','Mid Ulster','Mid Ulster','Derry City and Strabane','Derry City and Strabane','Causeway Coast and Glens','Mid Ulster','Causeway Coast and Glens','Causeway Coast and Glens','Causeway Coast and Glens','Causeway Coast and Glens','Causeway Coast and Glens','Causeway Coast and Glens','Newry, Mourne and Down','Armagh City, Banbridge and Craigavon','Armagh City, Banbridge and Craigavon','Armagh City, Banbridge and Craigavon','Armagh City, Banbridge and Craigavon','Armagh City, Banbridge and Craigavon','Lisburn and Castlereagh','Lisburn and Castlereagh','Mid Ulster','Mid Ulster','Mid Ulster','Mid Ulster','Fermanagh and Omagh','Mid Ulster','Mid Ulster','Mid Ulster','Mid Ulster','Mid Ulster','Mid Ulster','Fermanagh and Omagh','Derry City and Strabane','Newry, Mourne and Down','Fermanagh and Omagh','Fermanagh and Omagh',
    ],
})

def lambda_handler(event, context):
    # Get the secret
    sm = boto3.client('secretsmanager')
    secretobj = sm.get_secret_value(SecretId='ni-covid-tweets')
    secret = json.loads(secretobj['SecretString'])

    # Get the previous data file list from S3
    s3 = boto3.client('s3')
    keyname = secret['shared-vacc-index']
    status = S3_scraper_index(s3, secret['bucketname'], keyname)
    index = status.get_dict()

    tweet = '''{doses_24:,} COVID-19 vaccine doses registered in NI on {date}
\u2022 {f_24:,} first
\u2022 {s_24:,} second
\u2022 {pct_f:.0%}/{pct_s:.0%} dose mix

{total:,} total doses
\u2022 {total_f:,} first
\u2022 {total_s:,} second

Population (16 and over) vaccinated
\u2022 {pop_f}% first
\u2022 {pop_s}% second

{source}'''.format(
    doses_24=event['First Doses Registered'] + event['Second Doses Registered'],
    f_24=event['First Doses Registered'],
    s_24=event['Second Doses Registered'],
    total=event['Total Doses'],
    total_f=event['Total First Doses'],
    total_s=event['Total Second Doses'],
    pct_f=event['First Doses Registered'] / (event['First Doses Registered'] + event['Second Doses Registered']),
    pct_s=event['Second Doses Registered'] / (event['First Doses Registered'] + event['Second Doses Registered']),
    date=datetime.datetime.strptime(event['Last Updated'],'%Y-%m-%d').strftime('%A %-d %B %Y'),
    pop_f=event['First Doses pc'],
    pop_s=event['Second Doses pc'],
    source= 'https://coronavirus.data.gov.uk/' if event['Source']=='PHE' else 'https://covid-19.hscni.net/'
    )

    blocks = ['','','','']
    for i in range(20):
        if (i*5)+5 <= event['Second Doses pc']:
            blocks[i//5] += green_block
        elif (i*5)+5 <= event['First Doses pc']:
            blocks[i//5] += white_block
        else:
            blocks[i//5] += black_block
    tweet2 = '''Proportion of NI adults (16 and over) vaccinated against COVID-19:

{blocks0}
{blocks1}
{blocks2}
{blocks3}

One block is one person in 20

{green} - 2nd dose received
{white} - 1st dose received
{black} - no doses'''.format(
    blocks0=blocks[0],
    blocks1=blocks[1],
    blocks2=blocks[2],
    blocks3=blocks[3],
    green=green_block,
    white=white_block,
    black=black_block
)
    plots = []
    today = datetime.datetime.now().date()
    if today.weekday() == 4: # Friday - scrape and plot vaccinations per person by postcode district
        driver = get_chrome_driver()
        if driver is None:
            logging.error('Failed to start chrome')
        else:
            try:
                session = requests.Session()
                # Find the PowerBI URL from the HSCNI site
                url = 'https://covid-19.hscni.net/ni-covid-19-vaccinations-dashboard/'
                html = BeautifulSoup(get_url(session, url, 'text'),features="html.parser")
                url = html.find('iframe')['src']
                # Use selenium to get the PowerBI report
                driver.get(url)
                # Navigate to page 6 of the report
                for _ in range(2,7):
                    WebDriverWait(driver, 20).until(EC.element_to_be_clickable((By.CSS_SELECTOR, ".pbi-glyph-chevronrightmedium")))
                    time.sleep(3.0 + 3*(random.random()))
                    driver.find_element_by_css_selector(".pbi-glyph-chevronrightmedium").click()
                # Right click on the bubble chart
                WebDriverWait(driver, 20).until(EC.element_to_be_clickable((By.CLASS_NAME, "mapBubbles")))
                webdriver.ActionChains(driver).context_click(driver.find_element_by_class_name("mapBubbles")).perform()
                # Click show as table
                WebDriverWait(driver, 20).until(EC.visibility_of_element_located((By.XPATH, "//*[contains(text(), 'Show as a table')]")))
                driver.find_element_by_xpath("//*[contains(text(), 'Show as a table')]").click()
                # Click on the table
                WebDriverWait(driver, 20).until(EC.element_to_be_clickable((By.CSS_SELECTOR, ".pivotTable .innerContainer .bodyCells div div div .pivotTableCellWrap")))
                toprow = driver.find_element_by_css_selector(".pivotTable .innerContainer .bodyCells div div div .pivotTableCellWrap")
                toprow.click()
                table = driver.find_element_by_css_selector(".pivotTable")
                items = [
                    my_elem.text for my_elem in WebDriverWait(
                        driver, 20).until(
                            EC.visibility_of_all_elements_located((
                                By.CSS_SELECTOR,
                                ".pivotTable .innerContainer .rowHeaders div div .pivotTableCellWrap, .pivotTable .innerContainer .bodyCells div div div .pivotTableCellWrap"
                            ))
                        )
                    ]
                headers = [item for item in items if item.startswith('BT')]
                cells = [item for item in items if not item.startswith('BT')]
                # Scroll down the table
                for i in range(len(headers)-2):
                    table.send_keys(Keys.DOWN)
                headers_new = [None]
                cells_new = []
                # Last scroll before bounce back should stay in the same place
                while headers_new[-1] not in headers:
                    if headers_new[-1] is not None:
                        headers.append(headers_new[-1])
                        cells.extend(get_new_items(cells, cells_new))
                    table.send_keys(Keys.DOWN)
                    items = [
                        my_elem.text for my_elem in WebDriverWait(
                            driver, 20).until(
                                EC.visibility_of_all_elements_located((
                                    By.CSS_SELECTOR,
                                    ".pivotTable .innerContainer .rowHeaders div div .pivotTableCellWrap, .pivotTable .innerContainer .bodyCells div div div .pivotTableCellWrap"
                                ))
                            )
                        ]
                    headers_new = [item for item in items if item.startswith('BT')]
                    cells_new = [item for item in items if not item.startswith('BT')]
                    # Table bounces back to the top if you scroll too many times
                    if (headers[0] == headers_new[0]):
                        logging.warning('Found first element so stopping')
                        break
                # Build/clean the data frame for plotting
                df = pandas.DataFrame({'Postcode District': headers, 'Vaccinations': cells})
                df['Vaccinations'] = df['Vaccinations'].str.replace(',','').astype(int)
                df['Postcode District'] = df['Postcode District'].str.replace('BT0','BT')
                df = df.groupby('Postcode District').sum().reset_index()
                df = df.merge(nisra_pops, how='left', right_on='Postcode District', left_on='Postcode District', validate='1:1')
                df['Vaccinations per Person'] = df['Vaccinations'] / df['Population']
                df['Vaccinations per Person over 20'] = df['Vaccinations'] / df['Population over 20']
                # Push the data calculated to s3
                stream = io.BytesIO()
                df.to_csv(stream, index=False)
                stream.seek(0)
                upload_key = '%s/%s/postcodes.csv' % (event['Last Updated'],keyname.rsplit('/',maxsplit=1)[0])
                s3.upload_fileobj(stream, secret['bucketname'], upload_key)
                # Create the row chart
                p = altair.vconcat(
                    altair.Chart(
                        df
                    ).mark_bar().encode(
                        x = altair.X('Vaccinations per Person:Q'),
                        y = altair.Y('Postcode District:O', sort='-x'),
                        color = altair.Color('Council Area:N'),
                    ).properties(
                        height=1000,
                        width=450,
                        title='NI COVID-19 Vaccinations per Person by Postcode District up until %s' %datetime.datetime.strptime(event['Last Updated'],'%Y-%m-%d').strftime('%-d %B %Y')
                    ),
                ).properties(
                    title=altair.TitleParams(
                        ['Vaccinations data from HSCNI COVID-19 dashboard, mid-2018 populations from NISRA',
                        'https://twitter.com/ni_covid19_data on %s'  %today.strftime('%A %-d %B %Y')],
                        baseline='bottom',
                        orient='bottom',
                        anchor='end',
                        fontWeight='normal',
                        fontSize=10,
                        dy=10
                    ),
                )
                plotname = 'vacc-postcodes-%s.png'%today.strftime('%Y-%d-%m')
                plotstore = io.BytesIO()
                p.save(fp=plotstore, format='png', method='selenium', webdriver=driver)
                plotstore.seek(0)
                plots.append({'name': plotname, 'store': plotstore})
            except:
                logging.exception('Caught exception in scraping/plotting')

    if event.get('notweet') is not True:
        api = TwitterAPI(secret['twitter_apikey'], secret['twitter_apisecretkey'], secret['twitter_accesstoken'], secret['twitter_accesstokensecret'])
        upload_ids = api.upload_multiple(plots)
        if event.get('testtweet') is True:
            if len(upload_ids) > 0:
                resp = api.dm(secret['twitter_dmaccount'], tweet, upload_ids[0])
            else:
                resp = api.dm(secret['twitter_dmaccount'], tweet)
            if len(upload_ids) > 1:
                resp = api.dm(secret['twitter_dmaccount'], tweet2, upload_ids[1])
            else:
                resp = api.dm(secret['twitter_dmaccount'], tweet2)
            message = 'Sent test DM'
        else:
            if len(upload_ids) > 0:
                resp = api.tweet(tweet, media_ids=upload_ids)
            else:
                resp = api.tweet(tweet)
            for i in range(len(index)):
                if index[i]['Last Updated'] == event['Last Updated']:
                    index[i]['tweet'] = resp.id
                    break
            status.put_dict(index)
            message = 'Tweeted ID %s and updated %s' %(resp.id, keyname)

            resp = api.tweet(tweet2, resp.id)
            message = 'Tweeted reply ID %s' %resp.id
    else:
        print(tweet)
        print(tweet2)
        message = 'Did not tweet'

    return {
        "statusCode": 200,
        "body": json.dumps({
            "message:": message,
        }),
    }
