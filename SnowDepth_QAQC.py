# This code attempts to QA/QC the snow depth data in a full year for all wx
# stations and all years

# Written by Julien Bodart (VIU) - 10/12/2023

import os
import pandas as pd 
import os.path
from sqlalchemy import create_engine
from datetime import datetime
import numpy as np
import matplotlib.pyplot as plt
from csv import writer
import re
from pathlib import Path
from itertools import groupby
import traceback

# function to find nearest date 
def nearest(items, pivot):
    return min(items, key=lambda x: abs(x - pivot))

# remove chained assignmnent warning from Python - be careful!
pd.set_option('mode.chained_assignment', None)

#%% Establish a connection with MySQL database 'viuhydro_wx_data_v2'
engine = create_engine('mysql+mysqlconnector://viuhydro_shiny:.rt_BKD_SB*Q@192.99.62.147:3306/viuhydro_wx_data_v2', echo = False)

#%% extract name of all tables within SQL database and clean up var name list
connection = engine.raw_connection()
cursor = connection.cursor()
cursor.execute("Show tables;")
wx_stations_lst = cursor.fetchall()
wx_stations = []
for i in range(len(wx_stations_lst)):
     lst = (re.sub(r'[^\w\s]', '', str(wx_stations_lst[i])))
     wx_stations.append(lst)
   
# remove 'raw' tables, remove all steph (but steph3), and others due to local issues
# or because there is no snowDepth sensor there, and sort out the name formatting
wx_stations = [x for x in wx_stations if "clean" in x ]
wx_stations = [x for x in wx_stations if not "legacy_ontree" in x] # remove legacy data for Cairnridgerun
wx_stations = [w.replace('clean_steph3', 'clean_Stephanie3') for w in wx_stations] # rename steph3 so it doesn't get cut out
wx_stations = [x for x in wx_stations if not "steph" in x] # remove all stephanies
wx_stations = [w.replace('clean_machmellkliniklini', 'clean_Machmellkliniklini') for w in wx_stations] # rename machmellkliniklini so it doesn't get cut out
wx_stations = [x for x in wx_stations if not "machmell" in x] # remove machmell from list
wx_stations = [x for x in wx_stations if not "russell" in x] # remove rennell from list
wx_stations = [w.replace('clean_Stephanie3', 'clean_steph3') for w in wx_stations] # rename steph3 back to original
wx_stations = [w.replace('clean_Machmellkliniklini', 'clean_machmellkliniklini') for w in wx_stations] # rename machmellkliniklini back to original
wx_stations_name = list(map(lambda st: str.replace(st, 'clean_', ''), wx_stations)) # remove 'clean_' for csv export
wx_stations_name_cap = [wx_name.capitalize() for wx_name in wx_stations_name] # capitalise station name

#%% Loop over each station at a time and clean up the snow depth variable
for l in range(len(wx_stations_name)):
    sql_database = wx_stations_name[l]
    sql_name = wx_stations_name_cap[l]
    
    print('###### Cleaning snow data for station: %s ######' %(sql_name))     
    
    # create new directory on Windows (if does not exist) and cd into it
    Path("D:/Vancouver_Island_University/Wx_station/wx_data_processing/QAQC/individual_figures/" + sql_database + "/SnowDepth").mkdir(parents=True, exist_ok=True)
    os.chdir("D:/Vancouver_Island_University/Wx_station/wx_data_processing/QAQC/individual_figures/" + sql_database + "/SnowDepth")
    
    #%% import current data on SQL database and clean name of some columns to match
    # CSV column names
    sql_file = pd.read_sql(sql="SELECT * FROM clean_" + sql_database, con = engine)
    
    #%% Make sure there is no gap in datetime (all dates are consecutive) and place
    # nans in all other values if any gaps are identified
    #deltas = sql_file['DateTime'].diff()[1:] # identify non-consecutive datetime
    #gaps = deltas[deltas > timedelta(days=1)] # spits out any gaps > one day
    df_dt = pd.Series.to_frame(sql_file['DateTime'])    
    sql_file = sql_file.set_index('DateTime').asfreq('1H').reset_index()
    dt_sql = pd.to_datetime(sql_file['DateTime'])
    
    #%% select variable you want to QA/QC
    var = 'Snow_Depth'
    var_name = 'Snow Depth'
    var_name_short = 'SnowDepth'
    
    #%% Loop over all years of weather station data and apply a QA/QC routine
    # get year range of dataset and loop through each year if these contain a full
    # 12-month water year
    yr_range = np.arange(dt_sql[1].year, datetime.now().year) # find min and max years
    
    # remove specific years in arrays due to issue with data quality in 'clean'
    if wx_stations_name[l] == 'steph3':
        yr_range = np.arange(int(yr_range[np.flatnonzero(yr_range == 2015)]),yr_range[-1])
        
    if wx_stations_name[l] == 'tetrahedron':
        yr_range = np.arange(int(yr_range[np.flatnonzero(yr_range == 2016)])+1,yr_range[-1])
        
    if wx_stations_name[l] == 'mountcayley':
        yr_range = np.delete(yr_range, np.flatnonzero(yr_range == 2022))
        
    for k in range(len(yr_range)):
        print('## Cleaning data for year: %d-%d ##' %(yr_range[k],yr_range[k]+1))      
        # calculate datetime for winter/summer and 12 months of that year
        # if exists (i.e. if there is a full 12 month water year (YYYY-10-01 to 
        # YYYY+1-09-30))
        try:
            # find summer, winter and annual datetimes
            dt_winter_yr = np.concatenate(([np.where(dt_sql == np.datetime64(datetime(yr_range[k], 10, 1, 00, 00, 00))), np.where(dt_sql == np.datetime64(datetime(yr_range[k]+1, 7, 31, 00, 00, 00)))]))
            dt_summer_yr = np.concatenate(([np.where(dt_sql == np.datetime64(datetime(yr_range[k]+1, 7, 1, 00, 00, 00))), np.where(dt_sql == np.datetime64(datetime(yr_range[k]+1, 9, 23, 00, 00, 00)))]))
            dt_yr = np.concatenate(([np.where(dt_sql == np.datetime64(datetime(yr_range[k], 10, 1, 00, 00, 00))), np.where(dt_sql == np.datetime64(datetime(yr_range[k]+1, 9, 30, 23, 00, 00)))]))

        # if not full year - find issue and attempt to fix it if there are some 
        # days missing at start or end of water year
        except ValueError:
            err_var = traceback.format_exc() # extract error message for variable        
            print('It appears that there is no summer/winter data for year: %d-%d - checking ...' %(yr_range[k],yr_range[k]+1))
            
            # find nearest date for both summer and winter compared to typical
            # year where winter starts YYYY-10-01 and summer ends YYYY+1-09-30
            start_yr_sql = nearest(dt_sql, datetime(yr_range[k], 10, 1, 00, 00, 00))
            end_yr_sql = nearest(dt_sql, datetime(yr_range[k+1], 9, 30, 23, 00, 00))

            # if the start of the winter for this dataset is not too different 
            # than the start of the winter for a typical year (i.e. YYYY-12-15), 
            # which would indicate a lack of early winter data for this dataset,
            # still continue with the loop with slightly truncated dataset at 
            # the start
            if 'dt_winter' in err_var and start_yr_sql <= datetime(yr_range[k], 12, 15, 00, 00, 00):
                print('# Shorter winter timeseries for year: %d-%d - going ahead #' %(yr_range[k],yr_range[k]+1))
                dt_yr = np.concatenate(([np.where(dt_sql == start_yr_sql), np.where(dt_sql == np.datetime64(datetime(yr_range[k]+1, 9, 30, 23, 00, 00)))]))
                dt_winter_yr = np.concatenate(([np.where(dt_sql == start_yr_sql), np.where(dt_sql == np.datetime64(datetime(yr_range[k]+1, 7, 31, 23, 00, 00)))]))
                dt_summer_yr = np.concatenate(([np.where(dt_sql == np.datetime64(datetime(yr_range[k]+1, 7, 1, 00, 00, 00))), np.where(dt_sql == np.datetime64(datetime(yr_range[k]+1, 9, 23, 00, 00, 00)))]))

            # if the end of the summer for this dataset is not too different 
            # than the end of the summer for a typical year (i.e. YYYY+1-09-30), 
            # which would indicate a lack of summer data towards the end of this 
            # dataset, still continue with the loop with slightly truncated 
            # dataset at the end
            elif 'dt_summer' in err_var and end_yr_sql >= datetime(yr_range[k+1], 7, 15, 00, 00, 00):
                print('# Shorter summer timeseries for year: %d-%d - going ahead #' %(yr_range[k],yr_range[k]+1))
                dt_yr = np.concatenate(([np.where(dt_sql == np.datetime64(datetime(yr_range[k], 10, 1, 00, 00, 00))), np.where(dt_sql == end_yr_sql)]))
                dt_summer_yr = np.concatenate(([np.where(dt_sql == np.datetime64(datetime(yr_range[k]+1, 7, 1, 00, 00, 00))), np.where(dt_sql == np.datetime64(datetime(yr_range[k]+1, 9, 23, 00, 00, 00)))]))
            
            # else if this difference is too big for either winter or summer
            # then skip the dataset completely and go to next year
            else:
                print('Full summer/winter data for year: %d-%d is lacking - exiting loop' %(yr_range[k],yr_range[k]+1))
                # write warning to csv for inventory
                with open("D:/Vancouver_Island_University/Wx_station/wx_data_processing/QAQC/individual_figures/" + sql_database + "/SnowDepth/messages_" + sql_name + ".csv", 'a', newline='') as f_object:
                    writer_object = writer(f_object)
                    writer_object.writerow([sql_name + ' ' + var_name + ': no full summer/winter data - not QA/QCed for year:', yr_range[k],yr_range[k]+1])
                    f_object.close()
                
                # skip to next iteration if error found
                continue
        
        # likewise, if snow depth is all NaNs or there is no yearly data
        # (e.g. Datlamen) then skip iteration
        if sql_file[var].isnull().all() or dt_yr.size == 0: # if all nans or there is no yearly data
            print('It appears that there is no snow depth data for year: %d-%d - exiting loop' %(yr_range[k],yr_range[k]+1))
            
            # write warning to csv for inventory
            with open("D:/Vancouver_Island_University/Wx_station/wx_data_processing/QAQC/individual_figures/" + sql_database + "/SnowDepth/QAQC_issues_" + sql_name + ".csv", 'a', newline='') as f_object:
                writer_object = writer(f_object)
                writer_object.writerow([sql_name + ' ' + var_name + ': lack of snow - not QA/QCed for year:', yr_range[k],yr_range[k]+1])
                f_object.close()
            
            # skip to next iteration if error found
            continue  

        # plot raw data from SQL 'clean_xxxx' database and save to file
        #fig = plt.figure()
        #plt.plot(sql_file['DateTime'].iloc[np.arange(dt_yr[0].item(),dt_yr[1].item()+1)],sql_file[var].iloc[np.arange(dt_yr[0].item(),dt_yr[1].item()+1)])
        #plt.title(sql_name + ' %s Raw WTYR %d-%d' %(var_name,yr_range[k],yr_range[k]+1))
        #plt.savefig('%d_%s %s Original-Raw WTYR %d-%d.png' %(k, sql_name, var_name_short,yr_range[k],yr_range[k]+1), dpi=400)
        #plt.close()
        #plt.clf()
        #plt.show()
        
        # store for plotting
        raw = sql_file[var].iloc[np.arange(dt_yr[0].item(),dt_yr[1].item()+1)]
        
        #%% Apply static range test (remove values where difference is > than value)
        # Maximum value between each step: 2 cm
        sql_step = 25 # in cm
        data = sql_file[var].iloc[np.arange(dt_yr[0].item(),dt_yr[1].item()+1)]
        
        for i in range(len(data)-1):
            if abs(data.iloc[i] - data.iloc[i-1]) > sql_step:
                idx = data.index[i]
                sql_file.loc[idx, var] = np.nan # place nans if diff is > 2 cm
        
        # plot static-test cleaned data
        #fig = plt.figure()
        #plt.plot(sql_file['DateTime'].iloc[np.arange(dt_yr[0].item(),dt_yr[1].item()+1)],sql_file[var].iloc[np.arange(dt_yr[0].item(),dt_yr[1].item()+1)])
        #plt.title(sql_name + ' %s Static (%d cm) WTYR %d-%d' %(var_name,sql_step, yr_range[k],yr_range[k]+1))
        #plt.close()
        #plt.show()
        
        # store for plotting
        step_test = sql_file[var].iloc[np.arange(dt_yr[0].item(),dt_yr[1].item()+1)]
        
        #%% Remove all negative values (non-sensical)
        zeroes = sql_file[var].iloc[np.arange(dt_yr[0].item(),dt_yr[1].item()+1)]
        for i in range(len(zeroes)-1):
            if zeroes.iloc[i] < 0:
                idx = zeroes.index[i]
                sql_file.loc[idx, var] = 0 # place zeros if negative values found
    
        #%% Remove duplicate consecutive values
        data = sql_file[var].iloc[np.arange(dt_yr[0].item(),dt_yr[1].item()+1)]
        for i in range(len(data)-1):
            if abs(data.iloc[i] - data.iloc[i-1]) == 0:
                idx = data.index[i]
                sql_file.loc[idx, var] = np.nan # place nans if duplicates found
        
        # plot duplicates-cleaned data
        #fig = plt.figure()
        #plt.plot(sql_file['DateTime'].iloc[np.arange(dt_yr[0].item(),dt_yr[1].item()+1)],sql_file[var].iloc[np.arange(dt_yr[0].item(),dt_yr[1].item()+1)])
        #plt.title(sql_name + ' %s Duplicate WTYR %d-%d' %(var_name,yr_range[k],yr_range[k]+1))
        #plt.close()
        #plt.show()
        
        # store for plotting
        duplicates = sql_file[var].iloc[np.arange(dt_yr[0].item(),dt_yr[1].item()+1)]
        
        #%% Remove outliers based on mean and std using a rolling window for each
        # month of the year
        # make sure you take into account leap years and months with 30/31 days
        # find first the indices for each month in the array
        for i in range(1,12):
            # months with 30 days
            try:
                dt_mth = np.concatenate(([np.where(dt_sql == np.datetime64(datetime(2022, i, 1, 00, 00, 00))), np.where(dt_sql == np.datetime64(datetime(2022, i, 30, 00, 00, 00)))]))
            except:
                pass
            # months with 31 days
            try:        
                dt_mth = np.concatenate(([np.where(dt_sql == np.datetime64(datetime(2022, i, 1, 00, 00, 00))), np.where(dt_sql == np.datetime64(datetime(2022, i, 31, 00, 00, 00)))]))
            except:
                pass
            # leap years with 29 days in February
            try:
                dt_mth = np.concatenate(([np.where(dt_sql == np.datetime64(datetime(2022, i, 1, 00, 00, 00))), np.where(dt_sql == np.datetime64(datetime(2022, i, 29, 00, 00, 00)))]))
            except:
                pass
            # non-leap years with 28 days in February
            try:
                dt_mth = np.concatenate(([np.where(dt_sql == np.datetime64(datetime(2022, i, 1, 00, 00, 00))), np.where(dt_sql == np.datetime64(datetime(2022, i, 28, 00, 00, 00)))]))
            except:
                pass
            
            # then, remove outliers if they are greater than the monthly mean + 
            # 1.5x the standard deviation
            data_mth = sql_file[var].iloc[np.arange(dt_mth[0].item(),dt_mth[1].item()+1)]
            outliers = data_mth[data_mth > data_mth.mean() + 1.5*(data_mth.std())]
            sql_file.loc[outliers.index, var] = np.nan # place nans if outliers
        
        # plot outliers-cleaned data
        #fig = plt.figure()
        #plt.plot(sql_file['DateTime'].iloc[np.arange(dt_yr[0].item(),dt_yr[1].item()+1)],sql_file[var].iloc[np.arange(dt_yr[0].item(),dt_yr[1].item()+1)])
        #plt.title(sql_name + ' %s Rolling Mean WTYR %d-%d' %(var_name,yr_range[k],yr_range[k]+1))
        #plt.close()
        #plt.show()
        
        # store for plotting
        outliers_first = sql_file[var].iloc[np.arange(dt_yr[0].item(),dt_yr[1].item()+1)]
                
        #%% Remove any last outliers using sliding window of 20 samples and 
        # calculating the difference between the value at [i] and the mean of 
        # sliding window which should not exceed a specific value
        # Maximum value between each step: 25 cm
        value_exceeded = 30 # in cm
        
        data = sql_file[var].iloc[np.arange(dt_yr[0].item(),dt_yr[1].item()+1)]
        idx_exist = (data.iloc[:].loc[data.isnull()==False].index.tolist()) # indices of existing values
        max_outliers = data[idx_exist] # only keep non-nan values
        
        # first apply window for i to i - 50
        for i in range(len(max_outliers)-20):
            window = max_outliers[i:i+20]
            if abs(max_outliers.iloc[i] - window.mean()) > value_exceeded:
                idx = max_outliers.index[i]
                sql_file.loc[idx, var] = np.nan # place nans if outliers
    
        # then apply window for i+50 to i to get remaining outliers    
        for i in range(20,len(max_outliers)):
            window = max_outliers[i-20:i]
            if abs(max_outliers.iloc[i] - window.mean()) > value_exceeded:
                idx = max_outliers.index[i]
                sql_file.loc[idx, var] = np.nan # place nans if outliers
                
        # plot remaining outliers-cleaned data
        #fig = plt.figure()
        #plt.plot(sql_file['DateTime'].iloc[np.arange(dt_yr[0].item(),dt_yr[1].item()+1)],sql_file[var].iloc[np.arange(dt_yr[0].item(),dt_yr[1].item()+1)])
        #plt.title(sql_name + ' %s Sliding Window %d WTYR %d-%d' %(var_name,value_exceeded,yr_range[k],yr_range[k]+1))
        #plt.close()
        #plt.show()
        
        # store for plotting
        outliers_last = sql_file[var].iloc[np.arange(dt_yr[0].item(),dt_yr[1].item()+1)]
        
        #%% Remove non-sensical non-zero values in summer for Snow Depth
        # Find all values below threshold, then find the longest consecutive 
        # list of these values (e.g. summer months) and replace them by 0
        # These values are all likely wrong and correspond to sensor drift,
        # vegetation change, site visits, etc.
        data = sql_file[var].iloc[np.arange(dt_yr[0].item(),dt_yr[1].item()+1)]
        data_summer = sql_file[var].iloc[np.arange(dt_summer_yr[0].item(),dt_summer_yr[1].item()+1)]
        
        # calculate a maximum acceptable threshold - either mean value in summer 
        # months or if this is too small, a specific value (suggested to be 
        # 12 cm) based on eyeballing of the data in other wx stations or years
        mean_value_summer = np.mean(data_summer)
        arbitrary_value = 12
        threshold = mean_value_summer > arbitrary_value # check whichever is >
        
        if threshold == True: # if mean is bigger, then use this as threshold
            data_bool = sql_file[var].iloc[np.arange(dt_yr[0].item(),dt_yr[1].item()+1)] < mean_value_summer
            
        else: # else if mean is smaller, then use arbitrary value as threshold
            data_bool = sql_file[var].iloc[np.arange(dt_yr[0].item(),dt_yr[1].item()+1)] < arbitrary_value
        data_bool = data_bool.replace({True:1, False:0})
        data_bool[data[data.isnull()].index] = 1 # replace nans with 1
        
        # find index of longest sequence, making sure you're not picking up
        # a longer sequence at the start of the timeseries (e.g. in early winter)
        # hence the "data_bool.iloc[0:round(len(data)/2)]"
        # which is used arbitrarily so that it does not pick up indices earlier than
        # Spring onwards
        data_bool.iloc[0:round(len(data)/2)] = 0
        idx_longest_sequence = data_bool.index[max(((lambda y: (y[0][0], len(y)))(list(g)) for k, g in groupby(enumerate(data_bool==1), lambda x: x[1]) if k), key=lambda z: z[1])[0]]
        sql_file[var].iloc[np.arange(idx_longest_sequence,dt_yr[1].item()+1)] = 0
        
        # store for plotting
        summer_zeroing = sql_file[var].iloc[np.arange(dt_yr[0].item(),dt_yr[1].item()+1)]

        #%% one more pass to correct remaining outliers using the step size
        # and different levels until it's all 'shaved off'
        sql_steps = [20,15,10,5] # in cm
        for h in range(len(sql_steps)):
            sql_step = sql_steps[h] # in cm
            data = sql_file[var].iloc[np.arange(dt_yr[0].item(),dt_yr[1].item()+1)]
            idx_exist = (data.iloc[:].loc[data.isnull()==False].index.tolist()) # indices of existing values
            data = data[idx_exist]
            
            for i in range(len(data)-1):
                if abs(data[data.index[i]] - data[data.index[i-1]]) > sql_step:
                    idx = data.index[i]
                    sql_file.loc[idx, var] = np.nan # place nans if diff is > 2 cm
            
        # plot static-test cleaned data
        #fig = plt.figure()
        #plt.plot(sql_file['DateTime'].iloc[np.arange(dt_yr[0].item(),dt_yr[1].item()+1)],sql_file[var].iloc[np.arange(dt_yr[0].item(),dt_yr[1].item()+1)])
        #plt.title(sql_name + ' %s Static (%d cm) WTYR %d-%d' %(var_name,sql_step, yr_range[k],yr_range[k]+1))
        #plt.close()
        #plt.show()
            
        # store for plotting
        pre_interpolation = sql_file[var].iloc[np.arange(dt_yr[0].item(),dt_yr[1].item()+1)]
        
        #%% Find consecutive nans to see how much you could interpolate
        pre_interp_bool = np.isnan(pre_interpolation)
        pre_interp_bool = pre_interp_bool.replace({True:1, False:0})
        
        idxs = np.flatnonzero(pre_interp_bool) # get indices of non zero elements
        idxs = pre_interpolation.index[0] + idxs # add index in reference to array
        values = pre_interp_bool[idxs]
        counts = np.diff(idxs, append=len(pre_interp_bool))  
        
        #%% Interpolate nans with method='linear' using pandas.DataFrame.interpolate
        # First, identify gaps larger than 3 hours (which should not be interpolated)
        df = sql_file[var].iloc[np.arange(dt_yr[0].item(),dt_yr[1].item()+1)]
        #max_hours = 168 # 7 days
        max_hours = 3 # 3 hours
        mask = df.isna()
        mask = (mask.groupby((mask != mask.shift()).cumsum()).transform(lambda x: len(x) > max_hours)* mask)
        #print(len([i for i, x in enumerate(mask) if x])) # get length of gaps > 48 hrs
    
        #index = dt_yr[0].item() + np.where(sql_file[var].iloc[np.arange(dt_yr[0].item(),dt_yr[1].item()+1)].isnull())[0] # find indices of nans in reduced array
        index = df[np.logical_or(mask == 0, df == np.nan)].index
        interpolated = sql_file[var].iloc[np.arange(dt_yr[0].item(),dt_yr[1].item()+1)].interpolate() # interpolate all nans
        sql_file.loc[index, var] = interpolated[index] # place newly interpolated values into the master array
    
        # plot final interpolated and QA/QC-ed data and save to file
        #fig = plt.figure()
        #plt.plot(sql_file['DateTime'].iloc[np.arange(dt_yr[0].item(),dt_yr[1].item()+1)],sql_file[var].iloc[np.arange(dt_yr[0].item(),dt_yr[1].item()+1)])
        #plt.title(sql_name + ' %s Interpolated QA-QC WTYR %d-%d' %(var_name, yr_range[k],yr_range[k]+1))
        #plt.savefig('%d_%s %s QA-QC Interpolated WTYR %d-%d.png' %(k,sql_name,var_name_short,yr_range[k],yr_range[k]+1), dpi=400)
        #plt.close()
        #plt.clf()
        #plt.show() 
        
        # store for plotting
        interpolated = sql_file[var].iloc[np.arange(dt_yr[0].item(),dt_yr[1].item()+1)]
        
        #%% plot raw vs QA/QC
        fig, ax = plt.subplots()
        #ax.plot(sql_file['DateTime'].iloc[np.arange(dt_yr[0].item(),dt_yr[1].item()+1)],raw, '#1f77b4',marker='o') # blue
        #ax.plot(sql_file['DateTime'].iloc[np.arange(dt_yr[0].item(),dt_yr[1].item()+1)],step_test, '#ff7f0e',marker='o') # orange
        #ax.plot(sql_file['DateTime'].iloc[np.arange(dt_yr[0].item(),dt_yr[1].item()+1)],outliers_first, '#ef136f',marker='o') # pink
        #ax.plot(sql_file['DateTime'].iloc[np.arange(dt_yr[0].item(),dt_yr[1].item()+1)],outliers_last, '#d62728',marker='o') # red
        #ax.plot(sql_file['DateTime'].iloc[np.arange(dt_yr[0].item(),dt_yr[1].item()+1)],interpolated,'#77b41f', marker='o') # green
        
        ax.plot(sql_file['DateTime'].iloc[np.arange(dt_yr[0].item(),dt_yr[1].item()+1)],raw, '#1f77b4') # blue
        ax.plot(sql_file['DateTime'].iloc[np.arange(dt_yr[0].item(),dt_yr[1].item()+1)],interpolated, '#d62728')
        ax.plot(sql_file['DateTime'].iloc[np.arange(dt_yr[0].item(),dt_yr[1].item()+1)],pre_interpolation, '#ff7f0e')
        #plt.show()
        plt.title(sql_name + ' %s QA-QC WTYR %d-%d' %(var_name, yr_range[k],yr_range[k]+1))
        plt.savefig('%s %s Final Comparison WTYR %d-%d.png' %(sql_name,var_name_short,yr_range[k],yr_range[k]+1), dpi=400)
        plt.close()
        plt.clf()
        
    # %% to save to MySQL database
    
    # =============================================================================
    # # calculate water year (new year starts on 10.01.YYYY). If months are 
    # # before October, do nothing. Else add +1
    # WatYrs = []
    # for i in range(len(dt_sql)):
    #     if int(str(dt_sql.iloc[i]).split('-')[1]) < 10:
    #         WatYr = int(str(dt_sql.iloc[i]).split('-')[0])
    #     else:
    #         WatYr = int(str(dt_sql.iloc[i]).split('-')[0])+1
    #     WatYrs.append(WatYr)  
    #     
    # # create dataframe with rows to export to sql database
    # new_df = pd.DataFrame({'DateTime': dt_sql,
    #             'WatYr':WatYrs,
    #             'Air_Temp':sql_file['Temp'].astype(float),
    #             'RH':sql_file['RH'].astype(float),
    #             'BP':(sql_file['BP']/10).astype(float), # convert from hpa to kpa
    #             'Wind_speed':sql_file['Wspd'].astype(float),
    #             'Wind_Dir':sql_file['Dir'].astype(float),
    #             'Pk_Wind_Speed':sql_file['Mx_Spd'].astype(float), 
    #             'Pk_Wind_Dir':sql_file['Mx_Dir'],
    #             'PC_Tipper':sql_file['RnTotal'],
    #             'PP_Tipper':sql_file['Rn_1'].astype(float),
    #             'PC_Raw_Pipe':sql_file['PC'].astype(float),
    #             'Snow_Depth':sql_file['SDepth'].astype(float),
    #             'SWE':sql_file['SW'].astype(float),
    #             'Soil_Temperature':sql_file['ST'].astype(float),
    #             'Soil_Moisture':sql_file['SM'],
    #             'Solar_Rad':sql_file['PYR'].astype(float),
    #             'Batt':sql_file['Vtx'].astype(float),
    #             })
    # 
    # # push to database
    # #new_df.to_sql(name='filtered_tetrahedron', con=engine, if_exists = 'append', index=False)
    # =============================================================================
