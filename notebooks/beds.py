# %% [markdown]
# # FOI-based hospital/ICU beds data analysis
import pandas
import altair

altair.data_transformers.disable_max_rows()

# %% [markdown]
# ## BHSCT FOI data
#
# * weekly totals, beds data is summed (i.e. bed days)
bhsct_beds = pandas.read_excel('../data/BHSCT/10-11330 Available_Occupied Beds & ED Atts 2010 - 2020.xlsx', engine='openpyxl', header=[9,10,11], index_col=0, sheet_name='BEDS')
bhsct_beds = bhsct_beds.stack([0,2]).reset_index()
bhsct_beds.rename(columns={'level_0':'Dates','level_1':'Hospital','Dates':'Care'},inplace=True)
bhsct_beds['start'] = pandas.to_datetime(bhsct_beds['Dates'].str.split(' - ', expand=True)[0], format='%d/%m/%Y')
bhsct_beds = bhsct_beds.groupby(['start','Care','Hospital'])['Available', 'Occupied'].sum().reset_index()
bhsct_beds = bhsct_beds.melt(id_vars=['start','Care','Hospital'])
bhsct_beds['col'] = bhsct_beds['Care'] + '-' + bhsct_beds['variable']
bhsct_beds = bhsct_beds.pivot(index=['start','Hospital'], columns='col', values='value')
bhsct_beds.rename(columns={'ICU/Critical Care-Available': 'Critical Care Available', 'NON ICU/Critical Care-Available': 'General Available', 'ICU/Critical Care-Occupied': 'Critical Care Occupied', 'NON ICU/Critical Care-Occupied': 'General Occupied'}, inplace=True)
bhsct_ae = pandas.read_excel('../data/BHSCT/10-11330 Available_Occupied Beds & ED Atts 2010 - 2020.xlsx', engine='openpyxl', header=6, sheet_name='AE')
bhsct_ae['start'] = pandas.to_datetime(bhsct_ae['Dates'].str.split(' - ', expand=True)[0], format='%d/%m/%Y')
bhsct_ae.drop(columns=['Dates'],inplace=True)
bhsct_ae = bhsct_ae.melt(id_vars=['start']).groupby(['start','variable'])['value'].sum().reset_index()
bhsct_ae.rename(columns={'variable': 'Hospital', 'value': 'ED Attendances'}, inplace=True)
bhsct_ae.set_index(['start', 'Hospital'], inplace=True)
bhsct_weekly = bhsct_beds.merge(bhsct_ae, how='left', left_index=True, right_index=True)
bhsct_weekly.fillna(0, inplace=True)
bhsct_weekly = bhsct_weekly.astype(int)
bhsct_weekly = bhsct_weekly.reset_index().replace({
    'MIH': 'Mater Infirmorum Hospital',
    'RBHSC': 'Royal Belfast Hospital for Sick Children',
    'RVH': 'Royal Victoria Hospital',
    'BCH': 'Belfast City Hospital',
    'MPH': 'Musgrave Park Hospital'
}).set_index(['start','Hospital'])

# %% [markdown]
# ## NHSCT FOI data
#
# * daily data
nhsct_ae = pandas.read_excel('../data/NHSCT/20210208_PB080121_Response_Attachment_IJ.xlsx', engine='openpyxl', header=6, sheet_name='ED Attendances')
nhsct_ae.dropna(axis='columns', how='all', inplace=True)
nhsct_ae.dropna(axis='index', subset=['Arrival Date'], inplace=True)
nhsct_ae['date'] = pandas.to_datetime(nhsct_ae['Arrival Date'], format='%Y-%m-%d')
nhsct_ae.rename(columns={'Site': 'Hospital'}, inplace=True)
nhsct_ae_daily = nhsct_ae.groupby(['date','Hospital'])['Attendances'].sum()
nhsct_ae_daily.name = 'ED Attendances'
nhsct_icu = pandas.read_excel('../data/NHSCT/20210208_PB080121_Response_Attachment_IJ.xlsx', engine='openpyxl', header=5, sheet_name='ICU Wards')
nhsct_icu['date'] = pandas.to_datetime(nhsct_icu['DATE'], format='%Y-%m-%d')
nhsct_icu.rename(columns={'HOSPITAL': 'Hospital'}, inplace=True)
nhsct_icu_daily = nhsct_icu.groupby(['date','Hospital'])['AVAILABLE BEDS','OCCUPIED BEDS'].sum()
nhsct_icu_daily.rename(columns={'AVAILABLE BEDS': 'Critical Care Available', 'OCCUPIED BEDS': 'Critical Care Occupied'}, inplace=True)
nhsct_daily = nhsct_icu_daily.merge(nhsct_ae_daily, how='left', left_index=True, right_index=True)
nhsct_nonicu = pandas.read_excel('../data/NHSCT/20210208_PB080121_Response_Attachment_IJ.xlsx', engine='openpyxl', header=6, sheet_name='Non ICU Wards')
nhsct_nonicu['date'] = pandas.to_datetime(nhsct_nonicu['DATE'], format='%Y-%m-%d')
nhsct_nonicu.rename(columns={'HOSPITAL': 'Hospital'}, inplace=True)
nhsct_nonicu_daily = nhsct_nonicu.groupby(['date','Hospital'])['AVAILABLE BEDS','OCCUPIED BEDS'].sum()
nhsct_nonicu_daily.rename(columns={'AVAILABLE BEDS': 'General Available', 'OCCUPIED BEDS': 'General Occupied'}, inplace=True)
nhsct_daily = nhsct_daily.merge(nhsct_nonicu_daily, how='left', left_index=True, right_index=True)
nhsct_daily = nhsct_daily.astype(int)
nhsct_daily.reset_index(inplace=True)
nhsct_daily['start'] = nhsct_daily['date'] - pandas.to_timedelta((nhsct_daily['date'].dt.dayofweek+3)%7, unit='d')
nhsct_weekly = nhsct_daily.groupby(['start','Hospital']).sum()
nhsct_weekly = nhsct_weekly.reset_index().replace({'ANTRIM':'Antrim Area Hospital','CAUSEWAY':'Causeway Hospital'}).set_index(['start', 'Hospital'])

# %% [markdown]
# ## SEHSCT FOI data
#
# * weekly data, beds data is summed (i.e. bed days)
sehsct_beds = pandas.read_excel('../data/SEHSCT/Attachment 1 - Occupied & Available Beds and A&E Attendances.xlsx', engine='openpyxl', header=[9,10,11], sheet_name='Beds')
sehsct_beds.dropna(axis='columns', how='all', inplace=True)
sehsct_beds[sehsct_beds.columns[0]] = sehsct_beds[sehsct_beds.columns[0]].replace(to_replace ='27/03/2020 - 31/03/20', value = '2020-03-27 00:00:00')
sehsct_beds['start'] = pandas.to_datetime(sehsct_beds[sehsct_beds.columns[0]], format='%Y-%m-%d 00:00:00')
sehsct_beds.drop(columns=sehsct_beds.columns[0], inplace=True)
sehsct_beds = sehsct_beds.melt(id_vars=[('start','','')])
sehsct_beds.rename(columns={('start','',''): 'start', 'variable_0': 'hospital', 'variable_1': 'state', 'variable_2': 'ward'}, inplace=True)
sehsct_beds['col'] = sehsct_beds['ward'] + '-' + sehsct_beds['state']
sehsct_beds = sehsct_beds.pivot(index=['start', 'hospital'], columns='col', values='value').reset_index(1)
sehsct_beds.rename(columns={'ICU/Critical Care-Available': 'Critical Care Available', 'Non Critical Care-Available': 'General Available', 'ICU/Critical Care-Occupied': 'Critical Care Occupied', 'Non Critical Care-Occupied': 'General Occupied'}, inplace=True)
sehsct_beds.fillna(0, inplace=True)
sehsct_beds.rename(columns={'hospital': 'Hospital'}, inplace=True)
sehsct_weekly = sehsct_beds.groupby(['start','Hospital']).sum()
sehsct_ae = pandas.read_excel('../data/SEHSCT/Attachment 1 - Occupied & Available Beds and A&E Attendances.xlsx', engine='openpyxl', header=7, sheet_name='ED')
sehsct_ae['Week'] = sehsct_ae['Week'].replace(to_replace ='27/03/2020 - 31/03/20', value = '2020-03-27 00:00:00')
sehsct_ae['start'] = pandas.to_datetime(sehsct_ae['Week'], format='%Y-%m-%d 00:00:00')
sehsct_ae.drop(columns='Week', inplace=True)
sehsct_ae = sehsct_ae.melt(id_vars='start', var_name='Hospital').set_index(['start','Hospital'])
sehsct_ae['value'] = sehsct_ae['value'].fillna('0').replace(' ', '0').astype('int')
sehsct_ae = sehsct_ae.groupby(['start','Hospital'])['value'].sum()
sehsct_ae.name = 'ED Attendances'
sehsct_weekly = sehsct_weekly.merge(sehsct_ae, how='left', left_index=True, right_index=True)
sehsct_weekly.fillna(0, inplace=True)
sehsct_weekly = sehsct_weekly.astype(int)
sehsct_weekly = sehsct_weekly.reset_index().replace({
    'Ards': 'Ards Hospital',
    'Bangor': 'Bangor Hospital',
    'Downe': 'Downe Hospital',
    'Lagan Valley': 'Lagan Valley Hospital',
    'Ulster': 'Ulster Hospital'
}).set_index(['start', 'Hospital'])

# %% [markdown]
# ## SHSCT FOI data
#
# * daily data
shsct_ae = pandas.read_excel('../data/SHSCT/FOI 350 EC MIU ATTENDANCES.xlsx', engine='openpyxl', header=10, sheet_name='DATA')
shsct_ae['date'] = pandas.to_datetime(shsct_ae['Arrival Date'], format='%Y-%m-%d')
shsct_ae.rename(columns={'HOSPITAL':'Hospital'}, inplace=True)
shsct_ae.replace({'CRAIGAVON AREA HOSPITAL': 'Craigavon Area Hospital', 'DAISY HILL ': 'Daisy Hill Hospital', 'SOUTH TYRONE HOSPITAL': 'South Tyrone Hospital'}, inplace=True)
shsct_ae_daily = shsct_ae.groupby(['date','Hospital'])['No. of Attendances - Original'].sum()
shsct_ae_daily.name = 'ED Attendances'
shsct_icu = pandas.read_excel('../data/SHSCT/FOI 350 PARTS 1 AND 2 ALLOCATED AND OCCUPIED BEDS.xlsx', engine='openpyxl', header=11, sheet_name='INTENSIVE CARE BEDS')
shsct_icu.dropna(axis='index', subset=['Hospital Code'], inplace=True)
shsct_icu['date'] = pandas.to_datetime(shsct_icu['Date'], format='%Y-%m-%d 00:00:00')
shsct_icu.rename(columns={'Hospital Code':'Hospital'}, inplace=True)
shsct_icu_daily = shsct_icu.groupby(['date','Hospital'])['ALLOCATED','OCCUPIED'].sum()
shsct_icu_daily.rename(columns={'ALLOCATED': 'Critical Care Available', 'OCCUPIED': 'Critical Care Occupied'}, inplace=True)
shsct_nonicu = pandas.read_excel('../data/SHSCT/FOI 350 PARTS 1 AND 2 ALLOCATED AND OCCUPIED BEDS.xlsx', engine='openpyxl', header=11, sheet_name='OTHER BEDS')
shsct_nonicu.dropna(axis='index', subset=['Hospital Code'], inplace=True)
shsct_nonicu['date'] = pandas.to_datetime(shsct_nonicu['Date'], format='%Y-%m-%d 00:00:00')
shsct_nonicu.rename(columns={'Hospital Code':'Hospital'}, inplace=True)
shsct_nonicu_daily = shsct_nonicu.groupby(['date','Hospital'])['Allocated','Occupied'].sum()
shsct_nonicu_daily.rename(columns={'Allocated': 'General Available', 'Occupied': 'General Occupied'}, inplace=True)
shsct_daily = shsct_nonicu_daily.merge(shsct_icu_daily, how='left', left_index=True, right_index=True)
shsct_daily = shsct_daily.merge(shsct_ae_daily, how='left', left_index=True, right_index=True)
shsct_daily.fillna(0, inplace=True)
shsct_daily = shsct_daily.astype(int)
shsct_daily.reset_index(inplace=True)
shsct_daily['start'] = shsct_daily['date'] - pandas.to_timedelta((shsct_daily['date'].dt.dayofweek+3)%7, unit='d')
shsct_daily = shsct_daily[shsct_daily['date'] < '2020-04-01']
shsct_weekly = shsct_daily.groupby(['start','Hospital']).sum()

# %% [markdown]
# ## WHSCT FOI data
#
# * weekly data, beds data is summed (i.e. bed days)
whsct_ae = pandas.read_excel('../data/WHSCT/FOI.21.017 A&E Attendances and Bedday Activity ALT & SWA 2010 to 2020.xlsx', engine='openpyxl', header=5, sheet_name='AE')
whsct_ae['start'] = pandas.to_datetime(whsct_ae['Weekly Time Periods (7 Days)'].str.split(' - ', expand=True)[0], format='%d/%m/%Y')
whsct_ae.dropna(axis='index', how='all', inplace=True)
whsct_ae.drop(columns=['Weekly Time Periods (7 Days)'], inplace=True)
whsct_ae.rename(columns={'ALT': 'Altnagelvin Hospital', 'SWAH': 'South West Acute Hospital'}, inplace=True)
whsct_ae_weekly = whsct_ae.melt(id_vars='start', var_name='Hospital').groupby(['start','Hospital'])['value'].sum()
whsct_ae_weekly.name = 'ED Attendances'
whsct_beds = pandas.read_excel('../data/WHSCT/FOI.21.017 A&E Attendances and Bedday Activity ALT & SWA 2010 to 2020.xlsx', engine='openpyxl', header=[5,6,7], sheet_name='BEDS')
whsct_beds['start'] = pandas.to_datetime(whsct_beds[whsct_beds.columns[0]].str.split(' - ', expand=True)[0], format='%d/%m/%Y')
whsct_beds.drop(columns=whsct_beds.columns[0], inplace=True)
whsct_beds = whsct_beds.melt(id_vars=[('start','','')])
whsct_beds.rename(columns={('start','',''): 'start', 'variable_0': 'Hospital', 'variable_1': 'state', 'variable_2': 'ward'}, inplace=True)
whsct_beds['col'] = whsct_beds['ward'] + '-' + whsct_beds['state']
whsct_beds = whsct_beds.pivot(index=['start', 'Hospital'], columns='col', values='value').reset_index()
whsct_beds.replace({'ALTNAGELVIN HOSPITAL': 'Altnagelvin Hospital', 'ERNE / SOUTH WEST ACUTE HOSPITAL': 'South West Acute Hospital'}, inplace=True)
whsct_beds.rename(columns={'ICU/Critical Care-Available': 'Critical Care Available', 'NON ICU/Critical Care-Available': 'General Available', 'ICU/Critical Care-Occupied': 'Critical Care Occupied', 'NON ICU/Critical Care-Occupied': 'General Occupied'}, inplace=True)
whsct_beds.fillna(0, inplace=True)
whsct_weekly = whsct_beds.groupby(['start','Hospital']).sum()
whsct_weekly = whsct_weekly.merge(whsct_ae_weekly, how='left', left_index=True, right_index=True)
whsct_weekly.fillna(0, inplace=True)
whsct_weekly = whsct_weekly.astype(int)

# %% Combine all weekly into single dataframe
all_foi_weekly = pandas.concat([bhsct_weekly, nhsct_weekly, sehsct_weekly, shsct_weekly, whsct_weekly], keys=['BHSCT', 'NHSCT', 'SEHSCT', 'SHSCT', 'WHSCT'], names=['Trust', 'Week beginning', 'Hospital']).reset_index()
all_foi_weekly.to_csv('../data/all-foi-weekly.csv', index=False)

# %% Plot available ICU beds on a pre-COVID day
altair.Chart(
    (all_foi_weekly[all_foi_weekly['Week beginning']=='2020-01-03'].groupby(['Trust','Hospital']).sum() / 7).reset_index()
).mark_bar().encode(
    x=altair.X(field='Critical Care Available', type='quantitative', axis=altair.Axis(title='Critical Care Beds Available')),
    y=altair.Y(field='Hospital', type='ordinal'),
    color='Trust'
)

# %% Plot available ICU beds by Trust on a pre-COVID day
altair.Chart(
    (all_foi_weekly[all_foi_weekly['Week beginning']=='2020-01-03'].groupby(['Trust','Hospital']).sum() / 7).reset_index()
).mark_bar().encode(
    x=altair.X(field='Critical Care Available', type='quantitative', aggregate='sum', axis=altair.Axis(title='Critical Care Beds Available')),
    y=altair.Y(field='Trust', type='ordinal'),
    color='Trust'
)

# %% Plot available non-ICU beds on a pre-COVID day
altair.Chart(
    (all_foi_weekly[all_foi_weekly['Week beginning']=='2020-01-03'].groupby(['Trust','Hospital']).sum() / 7).reset_index()
).mark_bar().encode(
    x=altair.X(field='General Available', type='quantitative', axis=altair.Axis(title='General Beds Available')),
    y=altair.Y(field='Hospital', type='ordinal'),
    color='Trust'
)

# %%
altair.Chart(
    all_foi_weekly.groupby(['Week beginning', 'Trust']).sum().reset_index()
).mark_area().encode(
    x=altair.X(field='Week beginning', type='temporal'),
    y=altair.Y(field='Critical Care Occupied', type='quantitative', aggregate='sum', axis=altair.Axis(format=',d', title='Critical Care Occupied Bed Days')),
    color='Trust'
)

# %%
altair.Chart(
    all_foi_weekly.groupby(['Week beginning', 'Trust']).sum().reset_index()
).mark_area().encode(
    x=altair.X(field='Week beginning', type='temporal'),
    y=altair.Y(field='Critical Care Available', type='quantitative', aggregate='sum', axis=altair.Axis(format=',d', title='Critical Care Available Bed Days')),
    color='Trust'
)

# %% Trim the data to when data from all 5 trusts is available
all_foi_weekly_trimmed = all_foi_weekly[
    (all_foi_weekly['Week beginning'] >= all_foi_weekly[all_foi_weekly['Trust']=='SHSCT']['Week beginning'].min())
     & ((all_foi_weekly['Week beginning'] <= all_foi_weekly[all_foi_weekly['Trust']=='BHSCT']['Week beginning'].max()))
     ]

# %%
altair.Chart(
    all_foi_weekly_trimmed.groupby(['Week beginning', 'Trust']).sum().reset_index()
).mark_area().encode(
    x=altair.X(field='Week beginning', type='temporal'),
    y=altair.Y(field='Critical Care Occupied', type='quantitative', aggregate='sum', axis=altair.Axis(format=',d', title='Critical Care Occupied Bed Days')),
    color='Trust'
)

# %%
cc_hospitals = ['Altnagelvin Hospital','Antrim Area Hospital', 'Belfast City Hospital', 'Causeway Hospital', 'Craigavon Area Hospital', 'Mater Infirmorum Hospital', 'Royal Belfast Hospital for Sick Children', 'Royal Victoria Hospital', 'South West Acute Hospital', 'Ulster Hospital']

# %%
p = altair.Chart(
    all_foi_weekly_trimmed.groupby(['Week beginning', 'Trust']).sum().reset_index()
).mark_area().encode(
    x=altair.X(field='Week beginning', type='temporal'),
    y=altair.Y(field='Critical Care Available', type='quantitative', aggregate='sum', axis=altair.Axis(format=',d', title='Critical Care Available Bed Days')),
    color='Trust'
)

# %%
foi_icu_hospital = (all_foi_weekly_trimmed[all_foi_weekly_trimmed['Hospital'].isin(cc_hospitals)].groupby(['Week beginning', 'Hospital']).sum() / 7)[['Critical Care Occupied','Critical Care Available']].reset_index().melt(id_vars=['Hospital','Week beginning'],var_name=['Status'])
foi_icu_hospital['Status'] = foi_icu_hospital['Status'].replace({'Critical Care Available': 'Available', 'Critical Care Occupied': 'Occupied'})
p = altair.Chart(
    foi_icu_hospital[(foi_icu_hospital['Week beginning'] < foi_icu_hospital['Week beginning'].max()) & (foi_icu_hospital['Hospital'].isin(['Altnagelvin Hospital','Antrim Area Hospital']))]
).mark_line().encode(
    x=altair.X(field='Week beginning', type='temporal'),
    y=altair.Y(field='value', type='quantitative', aggregate='sum', axis=altair.Axis(format=',d', title='Critical Care Beds')),
    color='Status',
    row=altair.Row('Hospital',type='ordinal')
).properties(
    height=200
).resolve_scale(
    y='independent'
)
p.save('cc-alt-ant-%s.png'%datetime.datetime.now().date().strftime('%Y-%d-%m'))
p

# %%
foi_icu_trust = (all_foi_weekly_trimmed.groupby(['Week beginning', 'Trust']).sum() / 7)[['Critical Care Occupied','Critical Care Available']].reset_index().melt(id_vars=['Trust','Week beginning'],var_name=['Status'])
foi_icu_trust['Status'] = foi_icu_trust['Status'].replace({'Critical Care Available': 'Available', 'Critical Care Occupied': 'Occupied'})
p = altair.Chart(
    foi_icu_trust[(foi_icu_trust['Week beginning'] < foi_icu_trust['Week beginning'].max()) & (foi_icu_trust['Week beginning'] > foi_icu_trust['Week beginning'].min())]
).mark_line().encode(
    x=altair.X(field='Week beginning', type='temporal'),
    y=altair.Y(field='value', type='quantitative', aggregate='sum', axis=altair.Axis(format=',d', title='Critical Care Beds')),
    color=altair.Color('Status',title=None),
    row=altair.Row('Trust',type='ordinal')
).properties(
    height=90
).resolve_scale(
    y='independent'
)
p.save('cc-trusts-%s.png'%datetime.datetime.now().date().strftime('%Y-%d-%m'))
p

# %% [markdown]
# ## Load the COVID-era ICU/General beds data from the DoH sheet
#
# * Data needs to be interpolated to account for missing (Christmas/New Year)
# * General beds data needs to take into account the change in reporting on 18/10/2021
# * General beds is also interpolated to include the reporting change day
covid_icu = pandas.read_excel('../data/doh-dd-160621.xlsx', engine='openpyxl', sheet_name='ICU')
covid_icu.interpolate(inplace=True)
covid_icu['Critical Care Occupied'] = covid_icu['Confirmed COVID Occupied'] + covid_icu['Non COVID Occupied']
covid_icu['Critical Care Available'] = covid_icu['Confirmed COVID Occupied'] + covid_icu['Non COVID Occupied'] + covid_icu['Unoccupied Beds']
covid_icu['date'] = pandas.to_datetime(covid_icu['Date'], format='%Y-%m-%d')
covid_icu = covid_icu[['date','Critical Care Occupied','Critical Care Available']].set_index('date')
newind = pandas.date_range(start=covid_icu.index.min(), end=covid_icu.index.max())
covid_icu = covid_icu.reindex(newind).interpolate()
covid_nonicu = pandas.read_excel('../data/doh-dd-160621.xlsx', engine='openpyxl', sheet_name='General Beds')
covid_nonicu.dropna(axis='columns', how='all', inplace=True)
covid_nonicu['Confirmed COVID Occupied'] = covid_nonicu['Confirmed COVID Occupied'].replace('Break in Series - See Notes on Dashboard', 'NaN').astype('float')
covid_nonicu.interpolate(inplace=True)
covid_nonicu['date'] = pandas.to_datetime(covid_nonicu['Date'], format='%Y-%m-%d')
covid_nonicu.fillna(0, inplace=True)
covid_nonicu['General Occupied'] = covid_nonicu['Confirmed COVID Occupied'] + covid_nonicu['Non-COVID Occupied'] + covid_nonicu['Awaiting Admission']
covid_nonicu['General Available'] = covid_nonicu['Confirmed COVID Occupied'] + covid_nonicu['Non-COVID Occupied'] + covid_nonicu['Awaiting Admission'] + covid_nonicu['Unoccupied Beds']
covid_nonicu = covid_nonicu[['date','General Occupied','General Available']].set_index('date')
newind = pandas.date_range(start=covid_nonicu.index.min(), end=covid_nonicu.index.max())
covid_nonicu = covid_nonicu.reindex(newind).interpolate()
covid_daily = covid_icu.merge(covid_nonicu, how='left', left_index=True, right_index=True).reset_index()
covid_daily['Week beginning'] = covid_daily['index'] - pandas.to_timedelta((covid_daily['index'].dt.dayofweek+3)%7, unit='d')
covid_weekly = covid_daily.groupby('Week beginning').sum().reset_index()
covid_weekly['Trust'] = 'DoH'

# %% [markdown]
#
# ## Final tidy up
#
# Divide everything by 7
#
# Sort out the transition between the two datasets
#
# * BHSCT: 5 days in old data
# * NHSCT: 5 days in old data
# * SEHSCT: 5 days in old data
# * SHSCT: 5 days in old data
# * WHSCT: 5 days in old data
# * DoH: 2 or 3 days in first week
#
# Also strip off the final, incomplete week
all_weekly = pandas.concat([all_foi_weekly_trimmed.groupby('Week beginning').sum().reset_index(), covid_weekly])
all_weekly = (all_weekly.set_index(['Trust','Week beginning']) / 7.0).reset_index()
all_weekly['Year'] = all_weekly['Week beginning'].dt.year
all_weekly['Week'] = all_weekly['Week beginning'].dt.isocalendar().week
all_weekly = all_weekly[all_weekly['Week beginning'] < all_weekly['Week beginning'].max()]

# %%
all_icu = (all_weekly.groupby(['Week beginning','Week','Year']).sum())[['Critical Care Occupied','Critical Care Available']].reset_index().melt(id_vars=['Week beginning','Week','Year'],var_name=['Status'])
all_icu['Status'] = all_icu['Status'].replace({'Critical Care Available': 'Available', 'Critical Care Occupied': 'Occupied'})
p = altair.Chart(
    all_icu
).mark_line().encode(
    x=altair.X(r'monthdate(Week beginning):T', axis=altair.Axis(title='Week of year')),
    y=altair.Y(field='value', type='quantitative', aggregate='sum', axis=altair.Axis(format=',d', title='')),
    color=altair.Color('Status',title=None),
    row=altair.Row('Year',type='ordinal')
).properties(
    height=90,
    title='NI Critical Care Beds by Year'
)
p.save('cc-all-%s.png'%datetime.datetime.now().date().strftime('%Y-%d-%m'))
p


# %%
all_gen = (all_weekly.groupby(['Week beginning','Week','Year']).sum())[['General Occupied','General Available']].reset_index().melt(id_vars=['Week beginning','Week','Year'],var_name=['Status'])
all_gen['Status'] = all_gen['Status'].replace({'General Available': 'Available', 'General Occupied': 'Occupied'})
p = altair.Chart(
    all_gen
).mark_line().encode(
    x=altair.X(r'monthdate(Week beginning):T', axis=altair.Axis(title='Week of year')),
    y=altair.Y(field='value', type='quantitative', aggregate='sum', axis=altair.Axis(format=',d', title='')),
    color=altair.Color('Status',title=None),
    row=altair.Row('Year',type='ordinal')
).properties(
    height=90,
    title='NI General Beds by Year'
)
p.save('g-all-%s.png'%datetime.datetime.now().date().strftime('%Y-%d-%m'))
p

# %%
p = altair.Chart(
    all_gen[all_gen['Week beginning'] < '2020-02-01']
).mark_line().encode(
    x=altair.X(r'monthdate(Week beginning):T', axis=altair.Axis(title='Week of year')),
    y=altair.Y(
        field='value',
        type='quantitative',
        aggregate='sum',
        axis=altair.Axis(format=',d', title='General Beds'),
        scale=altair.Scale(zero=False),
    ),
    color=altair.Color('Status',title=None),
    row=altair.Row('Year',type='ordinal'),
).properties(
    height=90
)

# %%
all_gen['Reporting change'] = '2020-10-18'
reporting_change = [{'Date': '2020-10-18', 'event': 'Reporting change'}]
reporting_change = pandas.DataFrame(reporting_change)
p = altair.layer(
    altair.Chart(
        reporting_change
    ).mark_rule().encode(
        x=altair.X('Date:T', axis=altair.Axis(title=''))
    ),
    altair.Chart(
        reporting_change
    ).mark_text(
        dy=-5,
        angle=270
    ).encode(
        x=altair.X('Date:T', axis=altair.Axis(title='')),
        text='event'
    ),
    altair.Chart(
        all_gen[all_gen['Week beginning'] > '2020-02-01']
    ).mark_line().encode(
        x=altair.X('Week beginning:T'),
        y=altair.Y(
            field='value',
            type='quantitative',
            aggregate='sum',
            axis=altair.Axis(format=',d', title='General Beds'),
#            scale=altair.Scale(zero=False),
        ),
        color=altair.Color('Status',title=None)
    ),
).properties(
    height=450,
    width=800,
    title='NI general beds during COVID-19 pandemic',
)
p.save('g-covid-19-%s.png'%datetime.datetime.now().date().strftime('%Y-%d-%m'))
p


# %%
p = altair.Chart(
    all_icu[all_icu['Week beginning'] > '2020-02-01']
).mark_line().encode(
    x=altair.X('Week beginning:T'),
    y=altair.Y(field='value', type='quantitative', aggregate='sum', axis=altair.Axis(format=',d', title='Critical Care Beds')),
    color=altair.Color('Status',title=None)
).properties(
    height=450,
    width=800,
    title='NI critical care beds during COVID-19 pandemic',
)
p.save('cc-covid-19-%s.png'%datetime.datetime.now().date().strftime('%Y-%d-%m'))
p

# %%
pct_occ = all_weekly.groupby(['Week beginning','Week','Year']).sum()[['General Occupied','General Available']].reset_index()
pct_occ['value'] = pct_occ['General Occupied']/pct_occ['General Available']
p = altair.Chart(
    pct_occ[pct_occ['Week beginning'] > '2020-12-31']
).mark_line().encode(
    x=altair.X('Week beginning:T'),
    y=altair.Y(
        field='value',
        type='quantitative',
        axis=altair.Axis(format='%', title='Critical Care Beds % Occupied'),
        scale=altair.Scale(domain=[0.9,1])
    )
).properties(
    height=450,
    width=800,
    title='NI general beds % occupied in 2021',
)
p.save('g-2021-19-%s.png'%datetime.datetime.now().date().strftime('%Y-%d-%m'))
p

# %%
all_weekly[all_weekly['Week beginning']<'2020-02-01']['Critical Care Available'].max()

# %%
altair.vconcat(*[
    altair.Chart(
        all_weekly
    ).mark_line().encode(
        x=altair.X('monthdate(Week beginning):T', axis=None),
        y=altair.Y(field='Critical Care Occupied', type='quantitative', aggregate='sum', axis=altair.Axis(format=',d', title='Critical Care Occupied Beds')),
        color='Year:O'
    ),
    altair.Chart(
        all_weekly
    ).mark_line().encode(
        x=altair.X('monthdate(Week beginning):T', axis=None),
        y=altair.Y(field='Critical Care Available', type='quantitative', aggregate='sum', axis=altair.Axis(format=',d', title='Critical Care Available Beds')),
        color='Year:O'
    ),
    altair.Chart(
        all_weekly
    ).mark_line().encode(
        x=altair.X('monthdate(Week beginning):T', axis=None),
        y=altair.Y(field='General Available', type='quantitative', aggregate='sum', axis=altair.Axis(format=',d', title='Non-Critical Care Available Beds')),
        color='Year:O'
    ),
    altair.Chart(
        all_weekly
    ).mark_line().encode(
        x=altair.X('monthdate(Week beginning):T', axis=altair.Axis(title='Date')),
        y=altair.Y(field='General Occupied', type='quantitative', aggregate='sum', axis=altair.Axis(format=',d', title='Non-Critical Care Occupied Beds')),
        color='Year:O'
    )
])

# %%
