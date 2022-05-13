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

from shared import get_url, get_and_sort_index
from twitter_shared import TwitterAPI
from plot_shared import get_chrome_driver
from data_shared import get_eng_pop_pyramid, get_ni_pop_pyramid, update_datastore

good_symb = '\u2193'
bad_symb = '\u2191'

arrow_block = '\u2795'
green_block = '\u2705'
white_block = '\u2b1c'
black_block = '\u2b1b'

# List of NI age bands, with ordering for plotting
ni_age_bands_lookup = pandas.DataFrame([
    {'Order': 0, 'NI band': 'Under 5', 'Ages': [i for i in range(5)]},
    {'Order': 1, 'NI band': '5-11', 'Ages': [i for i in range(5,12)]},
    {'Order': 2, 'NI band': '12-15', 'Ages': [i for i in range(12,16)]},
    {'Order': 3, 'NI band': '16-17', 'Ages': [i for i in range(16,18)]},
    {'Order': 4, 'NI band': '18-29', 'Ages': [i for i in range(18,30)]},
    {'Order': 5, 'NI band': '30-39', 'Ages': [i for i in range(30,40)]},
    {'Order': 6, 'NI band': '40-49', 'Ages': [i for i in range(40,50)]},
    {'Order': 7, 'NI band': '50-59', 'Ages': [i for i in range(50,60)]},
    {'Order': 8, 'NI band': '60-69', 'Ages': [i for i in range(60,70)]},
    {'Order': 9, 'NI band': '70-79', 'Ages': [i for i in range(70,80)]},
    {'Order': 10, 'NI band': '80+', 'Ages': [i for i in range(80,91)]},
])

# List of comparable age bands, with ordering for plotting
all_age_bands_lookup = pandas.DataFrame([
    {'Order': 0, 'NI bands': ['Under 5'], 'Band': 'Under 5', 'Ages': [i for i in range(5)], 'Eng bands': ['Under 5']},
    {'Order': 1, 'NI bands': ['5-11'], 'Band': '5-11', 'Ages': [i for i in range(5, 11)], 'Eng bands': ['5-11']},
    {'Order': 2, 'NI bands': ['12-15'], 'Band': '12-15', 'Ages': [i for i in range(12,16)], 'Eng bands': ['12-15']},
    {'Order': 3, 'NI bands': ['16-17'], 'Band': '16-17', 'Ages': [i for i in range(16,18)], 'Eng bands': ['16-17']},
    {'Order': 4, 'NI bands': ['18-29'], 'Band': '18-29', 'Ages': [i for i in range(18,30)], 'Eng bands': ['18-24','25-29']},
    {'Order': 5, 'NI bands': ['30-39'], 'Band': '30-39', 'Ages': [i for i in range(30,40)], 'Eng bands': ['30-34','35-39']},
    {'Order': 6, 'NI bands': ['40-49'], 'Band': '40-49', 'Ages': [i for i in range(40,50)], 'Eng bands': ['40-44','45-49']},
    {'Order': 7, 'NI bands': ['50-59'], 'Band': '50-59', 'Ages': [i for i in range(50,60)], 'Eng bands': ['50-54','55-59']},
    {'Order': 8, 'NI bands': ['60-69'], 'Band': '60-69', 'Ages': [i for i in range(60,70)], 'Eng bands': ['60-64','65-69']},
    {'Order': 9, 'NI bands': ['70-79'], 'Band': '70-79', 'Ages': [i for i in range(70,80)], 'Eng bands': ['70-74','75-79']},
    {'Order': 10, 'NI bands': ['80+'], 'Band': '80+', 'Ages': [i for i in range(80,91)], 'Eng bands': ['80+']},
])

def get_ni_comparable_population_age_bands():
    age_bands_ons = all_age_bands_lookup.explode('Ages').reset_index()

    # Load the 2020 population data for NI and convert to the NI vaccine reporting bands
    ni_pop = get_ni_pop_pyramid()
    ni_pop = ni_pop[ni_pop['Year']==2020].groupby('Age Band').sum()['Population'].astype(int).reset_index()
    ni_pop = ni_pop.merge(age_bands_ons, how='inner', left_on='Age Band', right_on='Ages', validate='1:1')
    ni_pop = ni_pop.groupby(['Order','Band']).sum()['Population'].reset_index()
    ni_pop['% of total population'] = ni_pop['Population'] / ni_pop['Population'].sum()

    return ni_pop

def get_ni_reported_population_age_bands():
    age_bands_ons = ni_age_bands_lookup.explode('Ages').reset_index()

    # Load the 2020 population data for NI and convert to the NI vaccine reporting bands
    ni_pop = get_ni_pop_pyramid()
    ni_pop = ni_pop[ni_pop['Year']==2020].groupby('Age Band').sum()['Population'].astype(int).reset_index()
    ni_pop = ni_pop.merge(age_bands_ons, how='inner', left_on='Age Band', right_on='Ages', validate='1:1')
    ni_pop = ni_pop.groupby(['Order','NI band']).sum()['Population'].reset_index()
    ni_pop.rename(columns={'NI band': 'Band'}, inplace=True)
    ni_pop['% of total population'] = ni_pop['Population'] / ni_pop['Population'].sum()

    return ni_pop

def get_eng_population_age_bands():
    age_bands_ons = all_age_bands_lookup.explode('Ages').reset_index()

    # Load the 2020 population data for England and convert to the NI vaccine reporting bands
    eng_pop = get_eng_pop_pyramid()
    eng_pop = eng_pop[eng_pop['Year']==2020].groupby('Age Band').sum()['Population'].astype(int).reset_index()
    eng_pop = eng_pop.merge(age_bands_ons, how='inner', left_on='Age Band', right_on='Ages', validate='1:1')
    eng_pop = eng_pop.groupby(['Order','Band']).sum()['Population'].reset_index()
    eng_pop['% of total population'] = eng_pop['Population'] / eng_pop['Population'].sum()

    return eng_pop

def clean_eng_age_band_cols(x):
    return x.rstrip('0123456789,')

def get_eng_age_band_data():
    pop = get_eng_population_age_bands()
    age_bands = all_age_bands_lookup.explode('Eng bands').reset_index()
    # Check the NHS England page and find the latest age band Excel (includes under 18s, unlike the PHE API)
    session = requests.Session()
    url = 'https://www.england.nhs.uk/statistics/statistical-work-areas/covid-19-vaccinations/'
    html = BeautifulSoup(get_url(session, url, 'text'),features="html.parser")
    url = None
    for a in html.find_all("a"):
        if a['href'].endswith('.xlsx'):
            url = a['href']
            break
    if url is None:
        raise Exception('Unable to find England data')
    # Get the age band data, transform and aggregate
    try:
        eng = pandas.read_excel(url, sheet_name='Total Vaccinations by Age', header=12)
        eng.dropna(axis='columns', how='all', inplace=True)
        eng.dropna(axis='index', how='all', inplace=True)
        newcols = [clean_eng_age_band_cols(i) for i in eng.columns.values]
    except AttributeError:
        logging.exception('Falling back to earlier header row')
        eng = pandas.read_excel(url, sheet_name='Total Vaccinations by Age', header=11)
        eng.dropna(axis='columns', how='all', inplace=True)
        eng.dropna(axis='index', how='all', inplace=True)
        newcols = [clean_eng_age_band_cols(i) for i in eng.columns.values]
    eng.columns = newcols
    eng.dropna(axis='index', subset=['1st dose'], inplace=True)
    eng = eng.drop(columns=['Cumulative Total Doses to Date'])
    eng = eng[~eng['Age Group'].str.startswith('England')]
    eng.rename(columns={'Age Group': 'Age Band', '1st dose': 'First Doses', '2nd dose': 'Second Doses', 'Booster and 3rd dose': 'Booster and Third Doses'}, inplace=True)
    # Join to the population data and group by band
    eng = eng.merge(age_bands, how='inner', left_on='Age Band', right_on='Eng bands', validate='1:1')
    eng = eng.groupby(['Band']).sum()[['First Doses','Second Doses','Booster and Third Doses']].reset_index()
    eng = eng.merge(pop, how='inner', left_on='Band', right_on='Band', validate='1:1')
    eng['Nation'] = 'England'
    return eng

def pbi_goto_page(driver, pagenum):
    for _ in range(pagenum-1):
        WebDriverWait(driver, 20).until(EC.element_to_be_clickable((By.CSS_SELECTOR, ".pbi-glyph-chevronrightmedium")))
        time.sleep(3.0 + 3*(random.random()))
        driver.find_element_by_css_selector(".pbi-glyph-chevronrightmedium").click()

def get_ni_headline_data(driver, s3, bucketname, last_updated, s3_dir, store):
    headers = [
        my_elem.text for my_elem in WebDriverWait(
            driver, 20).until(
                EC.visibility_of_all_elements_located((
                    By.CSS_SELECTOR,
                    ".visualTitle"
                ))
            )
        ]
    items = [
        my_elem.find_element_by_tag_name('svg').get_attribute('aria-label') for my_elem in WebDriverWait(
            driver, 20).until(
                EC.visibility_of_all_elements_located((
                    By.CSS_SELECTOR,
                    ".visual-card"
                ))
            )
        ]
    df = pandas.DataFrame({'Dose': headers[1:], 'Total': items[1:len(headers)]})
    df['Total'] = df['Total'].str.replace(',','').str.extract(r'\s(\d+)').astype(int)
    df['Dose'] = df['Dose'].str.replace('\n',' ').str.extract(r'(Dose 1|Dose 2|Dose 3|Spring Booster|Booster)')
    keyname = '%s/doses.csv' % s3_dir
    datastore = update_datastore(s3, bucketname, keyname, last_updated, df, store)
    return datastore

def get_ni_age_band_data(driver, s3, bucketname, last_updated, s3_dir, store):
    pop = get_ni_comparable_population_age_bands()
    pop_reported = get_ni_reported_population_age_bands()
    age_bands = all_age_bands_lookup.explode('NI bands').reset_index()
    # Navigate to page 5 of the report
    pbi_goto_page(driver, 5)
    # Right click on the chart
    WebDriverWait(driver, 20).until(EC.element_to_be_clickable((By.CLASS_NAME, "visual-columnChart")))
    webdriver.ActionChains(driver).context_click(driver.find_element_by_class_name("visual-columnChart")).perform()
    # Click show as table
    WebDriverWait(driver, 20).until(EC.visibility_of_element_located((By.XPATH, "//*[contains(text(), 'Show as a table')]")))
    driver.find_element_by_xpath("//*[contains(text(), 'Show as a table')]").click()
    items = [
        my_elem.text for my_elem in WebDriverWait(
            driver, 20).until(
                EC.visibility_of_all_elements_located((
                    By.CSS_SELECTOR,
                    ".pivotTable .innerContainer .rowHeaders div div .pivotTableCellWrap, .pivotTable .innerContainer .bodyCells div div div .pivotTableCellWrap"
                ))
            )
        ]
    headers = [item for item in items if ('-' in item) or ('+' in item)]
    cells = [item for item in items if ('-' not in item) and ('+' not in item) and ('%' not in item)]
    ni = pandas.DataFrame({
        'Age Band': headers,
        'First Doses': cells[0:len(headers)],
        'Second Doses': cells[len(headers):len(headers)*2],
        'Third Doses': cells[len(headers)*2:len(headers)*3],
        'Booster Doses': cells[len(headers)*3:len(headers)*4],
    })
    ni['First Doses'] = ni['First Doses'].str.replace(',','').astype(int)
    ni['Second Doses'] = ni['Second Doses'].str.replace(',','').astype(int)
    ni['Third Doses'] = ni['Third Doses'].str.replace(r'^\s*$','0').str.replace(',','').astype(int)
    ni['Booster Doses'] = ni['Booster Doses'].str.replace(r'^\s*$','0').str.replace(',','').astype(int)
    # Combine into age bands
    ni = ni.merge(age_bands, how='inner', left_on='Age Band', right_on='NI bands', validate='1:1')
    ni_as_reported = ni.groupby(['Age Band']).sum()[['First Doses','Second Doses','Third Doses','Booster Doses']].reset_index()
    ni_as_reported = ni_as_reported.merge(pop_reported, how='right', left_on='Age Band', right_on='Band', validate='1:1')
    ni_as_reported = ni_as_reported[['Band', 'Order', 'First Doses', 'Second Doses', 'Third Doses', 'Booster Doses', 'Population', '% of total population']]
    # Update the s3 store
    keyname = '%s/agebands.csv' % s3_dir
    datastore = update_datastore(s3, bucketname, keyname, last_updated, ni_as_reported, store)
    previous_date = datastore[datastore['Date'] < datastore['Date'].max()]['Date'].max()
    previous = datastore[datastore['Date'] == previous_date][['Band', 'First Doses','Second Doses','Third Doses','Booster Doses']].rename(columns={'First Doses':'Previous First', 'Second Doses':'Previous Second', 'Third Doses':'Previous Third', 'Booster Doses': 'Previous Booster'})
    if len(previous) > 0:
        ni_as_reported = ni_as_reported.merge(previous, how='left', on='Band')
    # Combine with the comparable population data
    ni = ni.groupby(['Band']).sum()[['First Doses','Second Doses','Third Doses','Booster Doses']].reset_index()
    ni = ni.merge(pop, how='right', on='Band', validate='1:1')
    ni = ni[['Band', 'Order', 'First Doses', 'Second Doses', 'Third Doses', 'Booster Doses', 'Population', '% of total population']]
    ni['Nation'] = 'Northern Ireland'
    return ni, ni_as_reported

def make_age_band_plots(driver, ni, plots, today):
    eng = get_eng_age_band_data()
    ni['Booster and Third Doses'] = ni['Booster Doses'] + ni['Third Doses']
    df = pandas.concat([ni, eng])
    for key,value in {'First':'first','Second':'second','Booster and Third':'booster and third'}.items():
        df['Percentage %s doses' %value] = (df['%s Doses' % key]/df['Population']).clip(upper=1.0)
        df['%s doses as %% of total population' %key] = df['Percentage %s doses' %value] * df['% of total population']
        ni_done = df[df['Nation']=='Northern Ireland']['%s doses as %% of total population' %key].sum()
        eng_done = df[df['Nation']=='England']['%s doses as %% of total population' %key].sum()
        pct_diff = ni_done - eng_done
        p = altair.concat(
            altair.Chart(df).mark_bar(
                thickness=2,
                width=25,
                opacity=1
            ).encode(
                x=altair.X('Nation:O', axis=altair.Axis(labelAngle=0)),
                y=altair.Y('%s doses as %% of total population:Q' % key, aggregate='sum', axis=altair.Axis(format='%', title='Population received %s dose' % value)),
                color=altair.Color('Nation', legend=None)
            ).properties(
                width=600,
                title=altair.TitleParams(
                    text='NI has {val} dose vaccinated {pct_diff:.0%} {dir} of its population than England'.format(
                        pct_diff = abs(pct_diff),
                        dir = 'more' if (pct_diff > 0) else 'less',
                        val = value,
                    ),
                    subtitle=['NI has vaccinated {ni_done:.1%}, England {eng_done:.1%} for {val} doses'.format(
                        ni_done=ni_done,
                        eng_done=eng_done,
                        val=value,
                    )],
                    align='left',
                    anchor='start',
                    fontSize=18,
                    subtitleFontSize=14
                )
            )
        ).properties(
            title=altair.TitleParams(
                ['Population data for 2020 from ONS',
                'Vaccination data from HSCNI and NHS England',
                'https://twitter.com/ni_covid19_data on %s' %today.strftime('%Y-%m-%d')],
                baseline='bottom',
                orient='bottom',
                anchor='end',
                fontWeight='normal',
                fontSize=10,
                dy=10
            ),
        )
        plotname = 'vacc-ni-eng-%s-1-%s.png'%(value,today.strftime('%Y-%m-%d'))
        plotstore = io.BytesIO()
        p.save(fp=plotstore, format='png', method='selenium', webdriver=driver)
        plotstore.seek(0)
        plots.append({'name': plotname, 'store': plotstore})
    for key,value in {'First':'first','Second':'second','Booster and Third':'booster and third'}.items():
        p = altair.concat(
            altair.Chart(df).mark_bar(
                thickness=2,
                width=25,
                opacity=1
            ).encode(
                x=altair.X('Nation:O', axis=None),
                y=altair.Y('Percentage %s doses:Q' % value, axis=altair.Axis(format='%', title='%s doses completed' % key)),
                color='Nation',
                column=altair.Column('Band:O', sort=altair.SortField('Order'), header=altair.Header(title='Age Band', labelOrient='bottom', titleOrient='bottom'), spacing=0)
            ).properties(
                width=50,
                title=altair.TitleParams(
                    text='%s dose COVID-19 vaccine uptake for NI and England' %(value[0].upper() + value[1:]),
                    align='left',
                    anchor='start',
                    fontSize=18,
                    subtitleFontSize=15
                )
            )
        ).properties(
            title=altair.TitleParams(
                ['Population data for 2020 from ONS',
                'Vaccination data from HSCNI and NHS England',
                'https://twitter.com/ni_covid19_data on %s' %today.strftime('%Y-%m-%d')],
                baseline='bottom',
                orient='bottom',
                anchor='end',
                fontWeight='normal',
                fontSize=10,
                dy=10
            ),
        )
        plotname = 'vacc-ni-eng-%s-2-%s.png'%(value,today.strftime('%Y-%m-%d'))
        plotstore = io.BytesIO()
        p.save(fp=plotstore, format='png', method='selenium', webdriver=driver)
        plotstore.seek(0)
        plots.append({'name': plotname, 'store': plotstore})
    return plots

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

ni_postcode_pops = pandas.DataFrame({
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

def get_ni_postcode_data(driver, s3, bucketname, last_updated, s3_dir, store):
    # Navigate to page 9 of the report
    pbi_goto_page(driver, 9)
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
    df = df.merge(ni_postcode_pops, how='left', right_on='Postcode District', left_on='Postcode District', validate='1:1')
    df['Vaccinations per Person'] = df['Vaccinations'] / df['Population']
    df['Vaccinations per Person over 20'] = df['Vaccinations'] / df['Population over 20']
    df['Potential vaccinations'] = (df['Population over 20'] * 2) - df['Vaccinations']
    # Update the s3 store
    keyname = '%s/postcodes.csv' % s3_dir
    datastore = update_datastore(s3, bucketname, keyname, last_updated, df, store)
    return datastore

def make_postcode_plots(driver, datastore, plots, today, last_updated):
    # Calculate the NI vaccinations per person
    df = datastore[datastore['Date']==datastore['Date'].max()]
    df['colour'] = 'A'
    df.drop(columns='Date', inplace=True)
    df = df.append(
        {
            'Postcode District': 'NI',
            'Vaccinations per Person': df['Vaccinations'].sum() / df['Population'].sum(),
            'Vaccinations per Person over 20': df['Vaccinations'].sum() / df['Population over 20'].sum(),
            'Potential vaccinations': df['Potential vaccinations'].mean(),
            'colour': 'B'
        }, ignore_index=True)
    # Create the row chart for vaccinations per person
    vpp = altair.Chart(
        df
    ).mark_bar().encode(
        x = altair.X('Vaccinations per Person:Q'),
        y = altair.Y(
            'Postcode District:O',
            sort=altair.EncodingSortField(
                field='Vaccinations per Person',
                op='min',
                order='descending'
            )
        ),
        color = altair.Color('colour:N', legend=None),
    ).properties(
        height=1000,
        width=450,
        title='NI COVID-19 Vaccinations per Person by Postcode District up to %s' %last_updated.strftime('%-d %B %Y')
    )
    p = altair.vconcat(
        altair.layer(
            vpp,
            vpp.mark_text(
                align='left',
                baseline='middle',
                dx=3,
            ).encode(
                text = altair.Text('Vaccinations per Person:Q', format='.3r'),
            )
        )
    ).properties(
        title=altair.TitleParams(
            ['Vaccinations data from HSCNI COVID-19 dashboard, mid-2018 populations from NISRA',
            'Overall NI value is highlighted',
            'https://twitter.com/ni_covid19_data on %s'  %today.strftime('%A %-d %B %Y')],
            baseline='bottom',
            orient='bottom',
            anchor='end',
            fontWeight='normal',
            fontSize=10,
            dy=10
        ),
    )
    plotname = 'vacc-postcodes-%s.png'%today.strftime('%Y-%m-%d')
    plotstore = io.BytesIO()
    p.save(fp=plotstore, format='png', method='selenium', webdriver=driver)
    plotstore.seek(0)
    plots.append({'name': plotname, 'store': plotstore})
    # Create the row chart for most recent week
    last_reported = datastore[datastore['Date']!=datastore['Date'].max()]
    last_reported = last_reported[last_reported['Date']==last_reported['Date'].max()]
    last_reported.drop(columns='Date', inplace=True)
    change = df.merge(last_reported, how='inner', on='Postcode District', suffixes=('','_y'))
    change['Change'] = change['Vaccinations']-change['Vaccinations_y']
    p = altair.vconcat(
        altair.Chart(change).mark_bar().encode(
            y=altair.Y(
                'Postcode District:N',
                title='Postcode District (highest change at top, lowest at bottom)',
                sort=altair.SortField(
                    'Change',
                    'descending'
                )
            ),
            x=altair.X('Change:Q', title='New vaccinations last week')
        ).properties(
            height=1000,
            width=450,
            title='NI COVID-19 Vaccinations last week by Postcode District'
        )
    ).properties(
        title=altair.TitleParams(
            ['Vaccinations data from HSCNI COVID-19 dashboard',
            'https://twitter.com/ni_covid19_data on %s' %datetime.datetime.now().strftime('%A %-d %B %Y')],
            baseline='bottom',
            orient='bottom',
            anchor='end',
            fontWeight='normal',
            fontSize=10,
            dy=10
        ),
    )
    plotname = 'vacc-weekly-change-%s.png'%today.strftime('%Y-%m-%d')
    plotstore = io.BytesIO()
    p.save(fp=plotstore, format='png', method='selenium', webdriver=driver)
    plotstore.seek(0)
    plots.append({'name': plotname, 'store': plotstore})
    return plots

def make_headline_tweets(df, source, last_updated):
    latest = df[df['Date']==df['Date'].max()]
    previous = df[df['Date']!=df['Date'].max()]
    previous = previous[previous['Date']==previous['Date'].max()]
    total_reg = int(
        latest['Total'].sum() -
        latest[latest['Dose']=='Spring Booster']['Total'].sum() -
        previous['Total'].sum() +
        previous[previous['Dose']=='Spring Booster']['Total'].sum()
    )
    if total_reg <= 0:
        return None
    first_reg = int(latest[latest['Dose']=='Dose 1']['Total'].sum() - previous[previous['Dose']=='Dose 1']['Total'].sum())
    second_reg = int(latest[latest['Dose']=='Dose 2']['Total'].sum() - previous[previous['Dose']=='Dose 2']['Total'].sum())
    third_reg = int(latest[latest['Dose']=='Dose 3']['Total'].sum() - previous[previous['Dose']=='Dose 3']['Total'].sum())
    booster_reg = int(latest[latest['Dose']=='Booster']['Total'].sum() - previous[previous['Dose']=='Booster']['Total'].sum())
    sbooster_reg = int(latest[latest['Dose']=='Spring Booster']['Total'].sum() - previous[previous['Dose']=='Spring Booster']['Total'].sum())
    tweets = []
    tweets.append('''{doses_24:,} COVID-19 vaccinations reported in NI today
\u2022 {f_24:,} first
\u2022 {s_24:,} second
\u2022 {t_24:,} third
\u2022 {b_24:,} booster ({sb_24:,} spring)
\u2022 {pct_f}/{pct_s}/{pct_t}/{pct_b}% dose mix

{total:,} in total
\u2022 {total_f:,} first
\u2022 {total_s:,} second
\u2022 {total_t:,} third
\u2022 {total_b:,} booster ({total_sb:,} spring)

{source}'''.format(
        doses_24=total_reg,
        f_24=first_reg,
        s_24=second_reg,
        t_24=third_reg,
        b_24=booster_reg,
        sb_24=sbooster_reg,
        total=int(latest['Total'].sum() - latest[latest['Dose']=='Spring Booster']['Total'].sum()),
        total_f=int(latest[latest['Dose']=='Dose 1']['Total'].sum()),
        total_s=int(latest[latest['Dose']=='Dose 2']['Total'].sum()),
        total_t=int(latest[latest['Dose']=='Dose 3']['Total'].sum()),
        total_b=int(latest[latest['Dose']=='Booster']['Total'].sum()),
        total_sb=int(latest[latest['Dose']=='Spring Booster']['Total'].sum()),
        pct_f=int(first_reg/total_reg * 100),
        pct_s=int(second_reg/total_reg * 100),
        pct_t=int(third_reg/total_reg * 100),
        pct_b=int(booster_reg/total_reg * 100),
        date=last_updated.strftime('%A %-d %B %Y'),
        source= 'https://coronavirus.data.gov.uk/' if source=='PHE' else 'https://covid-19.hscni.net/ni-covid-19-vaccinations-dashboard/'
    ))

    blocks = ['','','','']
    for i in range(20):
        if (i*5)+5 <= (100 * (latest[latest['Dose']=='Dose 3']['Total'].sum()+latest[latest['Dose']=='Booster']['Total'].sum()-latest[latest['Dose']=='Spring Booster']['Total'].sum()) / 1597898):
            blocks[i//5] += arrow_block
        elif (i*5)+5 <= (100 * latest[latest['Dose']=='Dose 2']['Total'].sum() / 1597898):
            blocks[i//5] += green_block
        elif (i*5)+5 <= (100 * latest[latest['Dose']=='Dose 1']['Total'].sum() / 1597898):
            blocks[i//5] += white_block
        else:
            blocks[i//5] += black_block
    tweets.append('''Proportion of NI ages 12 and over vaccinated against COVID-19:

\u2022 {pop_f:.1%} first
\u2022 {pop_s:.1%} second
\u2022 {pop_b:.1%} third/booster (excluding spring)

{blocks0}
{blocks1}
{blocks2}
{blocks3}

One block is one person in 20

{arrow} - 3rd/booster
{green} - 2nd dose
{white} - 1st dose
{black} - no doses'''.format(
        pop_f=latest[latest['Dose']=='Dose 1']['Total'].sum() / 1597898,
        pop_s=latest[latest['Dose']=='Dose 2']['Total'].sum() / 1597898,
        pop_b=(latest[latest['Dose']=='Dose 3']['Total'].sum()+latest[latest['Dose']=='Booster']['Total'].sum()-latest[latest['Dose']=='Spring Booster']['Total'].sum()) / 1597898,
        blocks0=blocks[0],
        blocks1=blocks[1],
        blocks2=blocks[2],
        blocks3=blocks[3],
        arrow=arrow_block,
        green=green_block,
        white=white_block,
        black=black_block
    ))
    return tweets


def lambda_handler(event, context):
    message = 'Failure'
    try:
        # Get the secret
        sm = boto3.client('secretsmanager')
        secretobj = sm.get_secret_value(SecretId='ni-covid-tweets')
        secret = json.loads(secretobj['SecretString'])

        # Get the previous data file list from S3
        s3 = boto3.client('s3')
        keyname = secret['shared-vacc-index']
        index, indexobj = get_and_sort_index(secret['bucketname'], keyname, s3)

        tweets = []
        plots = []
        today = datetime.datetime.now().date()
        driver = get_chrome_driver()
        ni_age_bands_reported = None
        last_updated = datetime.datetime.now()
        print(last_updated)
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
                store_data = (event.get('notweet') is not True) and (event.get('testtweet') is not True)
                s3dir = keyname.rsplit('/',maxsplit=1)[0]
                headlines = get_ni_headline_data(driver, s3, secret['bucketname'], last_updated, s3dir, store_data)
                latest = headlines[headlines['Date']==headlines['Date'].max()]
                tweets = make_headline_tweets(headlines, 'HSCNI', last_updated)
                if tweets is None:
                    raise Exception('No change in data')
                ni_age_bands, ni_age_bands_reported = get_ni_age_band_data(driver, s3, secret['bucketname'], last_updated, s3dir, store_data)
                if today.weekday() == 5: # Saturday - Vaccinations per person by postcode district
                    driver.get(url)
                    postcodes = get_ni_postcode_data(driver, s3, secret['bucketname'], last_updated, s3dir, store_data)
                    plots = make_postcode_plots(driver, postcodes, plots, today, last_updated)
                elif today.weekday() == 0: # Monday - NI/Eng age band comparison
                    plots = make_age_band_plots(driver, ni_age_bands, plots, today)
            except:
                logging.exception('Caught exception in scraping/plotting')
        try:
            if ni_age_bands_reported is not None:
                tweets.append('First doses by age band\n\n')
                first = True
                for _,data in ni_age_bands_reported.to_dict('index').items():
                    fstring = '\u2022 {band}: {pct_done:.1%}'
                    if first:
                        fstring += ' of total'
                    if 'Previous First' in data:
                        fstring += ', {new:,}'
                        if first:
                            fstring += ' new'
                    fstring += '\n'
                    if not pandas.isna(data['First Doses']):
                        tweets[-1] += fstring.format(
                            band=data['Band'],
                            pct_done=data['First Doses']/data['Population'],
                            new=int(data['First Doses']-data.get('Previous First',0)),
                        )
                        first = False
                tweets.append('Second doses by age band\n\n')
                first = True
                for _,data in ni_age_bands_reported.to_dict('index').items():
                    if data['Second Doses'] > 0:
                        fstring = '\u2022 {band}: {pct_done:.1%}'
                        if first:
                            fstring += ' of total'
                        if ('Previous Second' in data) and (not pandas.isna(data['Second Doses'])):
                            fstring += ', {new:,}'
                            if first:
                                fstring += ' new'
                        fstring += '\n'
                        if not pandas.isna(data['Second Doses']):
                            if pandas.isna(data.get('Previous Second',0)):
                                change = 0
                            else:
                                change = data['Second Doses']-data.get('Previous Second',0)
                            tweets[-1] += fstring.format(
                                band=data['Band'],
                                pct_done=data['Second Doses']/data['Population'],
                                new=int(change),
                            )
                            first = False
                tweets.append('3rd/booster (not spring) age bands\n\n')
                first = True
                for _,data in ni_age_bands_reported.to_dict('index').items():
                    if (data['Third Doses'] + data['Booster Doses']) > 0:
                        fstring = '\u2022 {band}: {pct_done:.1%}'
                        if first:
                            fstring += ' of total'
                        if ('Previous Booster' in data) and (not pandas.isna(data['Booster Doses'])) and ('Previous Third' in data) and (not pandas.isna(data['Third Doses'])):
                            fstring += ', {new:,}'
                            if first:
                                fstring += ' new'
                        fstring += '\n'
                        if not pandas.isna(data['Third Doses']) and not pandas.isna(data['Booster Doses']):
                            if pandas.isna(data.get('Previous Third',0)):
                                change = 0
                            elif pandas.isna(data.get('Previous Booster',0)):
                                change = 0
                            else:
                                change = data['Third Doses']+data['Booster Doses']-data.get('Previous Third',0)-data.get('Previous Booster',0)
                            tweets[-1] += fstring.format(
                                band=data['Band'],
                                pct_done=(data['Third Doses']+data['Booster Doses'])/data['Population'],
                                new=int(change),
                            )
                            first = False
        except:
            logging.exception('Caught error in age band tweet')

        if event.get('notweet') is not True:
            api = TwitterAPI(secret['twitter_apikey'], secret['twitter_apisecretkey'], secret['twitter_accesstoken'], secret['twitter_accesstokensecret'])
            upload_ids = api.upload_multiple(plots)
            if event.get('testtweet') is True:
                for i in range(len(tweets)):
                    if (len(upload_ids) > i):
                        resp = api.dm(secret['twitter_dmaccount'], tweets[i], upload_ids[i])
                    else:
                        resp = api.dm(secret['twitter_dmaccount'], tweets[i])
                message = 'Sent test DM'
            else:
                for j in range(len(tweets)):
                    if j == 0:
                        if len(upload_ids) <= 4:
                            resp = api.tweet(tweets[0], media_ids=upload_ids)
                        else:
                            resp = api.tweet(tweets[0])
                        index_updated = False
                        datestr = datetime.datetime.strftime(last_updated, '%Y-%d-%m')
                        for i in range(len(index)):
                            if index[i]['Last Updated'] == datestr:
                                index[i]['tweet'] = resp.id
                                index_updated = True
                                break
                        if index_updated is False:
                            index.append({'tweet':resp.id, 'Last Updated': datestr, 'Total Doses': int(latest['Total'].sum())})
                        indexobj.put_dict(index)
                        message = 'Tweeted ID %s and updated %s' %(resp.id, keyname)
                    elif j == 1:
                        resp = api.tweet(tweets[j], resp.id)
                        message = 'Tweeted reply ID %s' %resp.id
                    elif j == 2:
                        if len(upload_ids) == 6:
                            resp = api.tweet(tweets[j], resp.id, media_ids=[upload_ids[0], upload_ids[3]])
                        else:
                            resp = api.tweet(tweets[j], resp.id)
                        message = 'Tweeted reply ID %s' %resp.id
                    elif j == 3:
                        if len(upload_ids) == 6:
                            resp = api.tweet(tweets[j], resp.id, media_ids=[upload_ids[1], upload_ids[4]])
                        else:
                            resp = api.tweet(tweets[j], resp.id)
                        message = 'Tweeted reply ID %s' %resp.id
                    elif j == 4:
                        if len(upload_ids) == 6:
                            resp = api.tweet(tweets[j], resp.id, media_ids=[upload_ids[2], upload_ids[5]])
                        else:
                            resp = api.tweet(tweets[j], resp.id)
                        message = 'Tweeted reply ID %s' %resp.id
                    else:
                        resp = api.tweet(tweets[j], resp.id)
                        message = 'Tweeted reply ID %s' %resp.id
        else:
            for tweet in tweets:
                print(tweet)
            message = 'Did not tweet'
    except:
        logging.exception('Caught error in vaccine tweeter')
        api = TwitterAPI(secret['twitter_apikey'], secret['twitter_apisecretkey'], secret['twitter_accesstoken'], secret['twitter_accesstokensecret'])
        api.dm(secret['twitter_dmaccount'], 'Error in vaccine tweeter')

    return {
        "statusCode": 200,
        "body": json.dumps({
            "message:": message,
        }),
    }
