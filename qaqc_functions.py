#%% Repository of all functions associated with the qa/qc of all wx station
# data, irrespective of wx station or wx variable

#%% Import functions
import os
import pandas as pd 
import numpy as np
from datetime import datetime
from itertools import groupby

#%% Static range test (result: FAIL if TRUE)
def static_range_test(data_all, data_subset, flag, step):
    flag_arr = pd.Series(np.zeros((len(data_all))))
    
    for i in range(len(data_subset)-1):
        if abs(data_subset.iloc[i] - data_subset.iloc[i-1]) > step:
            idx = data_subset.index[i]
            data_all[idx] = np.nan
            flag_arr[idx] = flag         
    return data_all, flag_arr

#%% shave off outliers (similar to static_range_test function but it repeats 
# the process for multiple steps)
def static_range_multiple(data_all, data_subset, flag, steps):
    flag_arr = pd.Series(np.zeros((len(data_all))))

    for h in range(len(steps)):
        step = steps[h]
        data = data_subset
        idx_exist = (data[data.isnull()==False].index.tolist()) # indices of existing values
        data = data[idx_exist]
        
        for i in range(len(data)-1):
            if abs(data[data.index[i]] - data[data.index[i-1]]) > step:
                idx = data.index[i]
                data_subset[idx] = np.nan
                data_all[idx] = np.nan
                flag_arr[idx] = flag         
    return data_all, flag_arr

#%% Remove duplicate values
def duplicates(data_all, data_subset, flag):
    flag_arr = pd.Series(np.zeros((len(data_all))))
    
    for i in range(len(data_subset)-1):
        if abs(data_subset.iloc[i] - data_subset.iloc[i-1]) == 0:
            idx = data_subset.index[i]
            data_all[idx] = np.nan
            flag_arr[idx] = flag        
    return data_all, flag_arr

#%% Breakpoint analysis to detect summer trend and zero out values after that (e.g. SWE)
def SWE_summer_zeroing(data_all, data_subset, dt_yr, dt_summer_yr, flag):
    flag_arr = pd.Series(np.zeros((len(data_all))))
    
    # find index in data of maximum gradient change
    slope_change_summer = np.gradient(data_subset)
    
    # find index of longest sequence, making sure you're not picking up
    # a longer sequence at the start of the timeseries (e.g. in early winter)
    # hence the "data_bool.iloc[0:round(len(data_subset)/2)]"
    # which is used arbitrarily so that it does not pick up indices earlier than
    # Spring onwards
    june_idx = (dt_summer_yr[0]-24*30) - data_subset.index[0] # index for 06-01 in slope_change_summer
    slope_change_summer[np.arange(0,june_idx)] = np.nan # all values before are nan
    idx_summer_sequence = np.nanargmin(slope_change_summer) + data_subset.index[0]  # index for summer sequence in data array
    
    # store for plotting
    idxs = np.arange(idx_summer_sequence,dt_yr[1].item()+1)
    data_all[idxs] = 0
    flag_arr[idxs] = flag          

    return data_all, flag_arr

#%% Remove non-sensical non-zero values in summer for snow depth variable
# Find all values below threshold, then find the longest consecutive 
# list of these values (e.g. summer months) and replace them by 0
# These values are all likely wrong and correspond to sensor drift,
# vegetation change, site visits, etc. Only caveat to this is that certain
# stations flatten out earlier in the summer, so the oode does not pick these 
# up well. Instead, a csv with dates when snow melt flattens around zero is imported
def sdepth_summer_zeroing(data_all, data_subset, flag, dt_yr, dt_summer_yr, summer_threshold, dt, wx_stations_name, year):
    flag_arr = pd.Series(np.zeros((len(data_all))))
    data_summer = data_all.iloc[np.arange(dt_summer_yr[0].item(),dt_summer_yr[1].item()+1)]

    # Read in the CSV containing specific summer dates for certain wx stations
    with open('D:/Vancouver_Island_University/Wx_station/wx_data_processing/QAQC/qaqc_dates/sdepth_zeroing_dates.csv', 'r') as readFile:
        df_csv = pd.read_csv(readFile,low_memory=False)
        csv_dt = pd.to_datetime(df_csv['zero_date'])
        df_csv['zero_date'] = csv_dt.dt.year.values
        
    # calculate a maximum acceptable threshold - either mean value in summer 
    # months or if this is too small, a specific value (suggested to be 
    # 12 cm) based on eyeballing of the data in other wx stations or years
    mean_value_summer = np.mean(data_summer)
    arbitrary_value = summer_threshold
    threshold = mean_value_summer > arbitrary_value # check whichever is >
    
    # if there is specific date in the csv, then run below
    name = pd.concat([pd.DataFrame([wx_stations_name],columns=['filename']), pd.DataFrame([year],columns=['zero_dates'])], axis=1, join='inner')
    if np.any((df_csv.values == name.values).all(axis=1)) == True:
        idx = int(np.flatnonzero((df_csv.values == name.values).all(axis=1)))
        idx_longest_sequence = int(np.flatnonzero((csv_dt[idx] == dt)))

    # else if there is no specific dates in the csv, then run below
    else:
        if threshold == True: # if mean is bigger, then use this as threshold
            data_bool = data_all.iloc[np.arange(dt_yr[0].item(),dt_yr[1].item()+1)] < mean_value_summer
            
        else: # else if mean is smaller, then use arbitrary value as threshold
            data_bool = data_all.iloc[np.arange(dt_yr[0].item(),dt_yr[1].item()+1)] < arbitrary_value
        data_bool = data_bool.replace({True:1, False:0})
        data_bool[data_subset[data_subset.isnull()].index] = 1 # replace nans with 1
        
        # find index of longest sequence, making sure you're not picking up
        # a longer sequence at the start of the timeseries (e.g. in early winter)
        # hence the "data_bool.iloc[0:round(len(data)/2)]"
        # which is used arbitrarily so that it does not pick up indices earlier than
        # Spring onwards
        data_bool.iloc[0:round(len(data_subset)/2)] = 0
        idx_longest_sequence = data_bool.index[max(((lambda y: (y[0][0], len(y)))(list(g)) for k, g in groupby(enumerate(data_bool==1), lambda x: x[1]) if k), key=lambda z: z[1])[0]]
    
    data_all[np.arange(idx_longest_sequence,dt_yr[1].item()+1)] = 0
    flag_arr[np.arange(idx_longest_sequence,dt_yr[1].item()+1)]  = flag          

    return data_all, flag_arr
    
#%% Remove values above the mean of a sliding window of sample length "window_len" 
def mean_sliding_window(data_all, data_subset, flag, window_len, mean_sliding_val):
    flag_arr = pd.Series(np.zeros((len(data_all))))
    idx_exist = (data_subset.iloc[:].loc[data_subset.isnull()==False].index.tolist()) # indices of existing values
    max_outliers = data_subset[idx_exist] # only keep non-nan values
    
    # first apply window for i to i-window_len
    for i in range(len(max_outliers)-window_len):
        window = max_outliers[i:i+window_len]
        if abs(max_outliers.iloc[i] - window.mean()) > mean_sliding_val:
            idx = max_outliers.index[i]
            data_all[idx] = np.nan # place nans if outliers
            flag_arr[idx] = flag          

    # then apply window for i+window_len to i to get remaining outliers    
    for i in range(window_len,len(max_outliers)):
        window = max_outliers[i-window_len:i]
        if abs(max_outliers.iloc[i] - window.mean()) > mean_sliding_val:
            idx = max_outliers.index[i]
            data_all[idx] = np.nan
            flag_arr[idx] = flag        
    
    return data_all, flag_arr

#%% Remove all negative values
def negtozero(data_all, data_subset, flag):
    flag_arr = pd.Series(np.zeros((len(data_all))))

    for i in range(len(data_subset)-1):
        if data_subset.iloc[i] < 0:
            idx = data_subset.index[i]
            data_all[idx] = 0 
            flag_arr[idx] = flag        
    
    return data_all, flag_arr

#%% Remove outliers based on mean and std using a rolling window for each
# month of the year
def mean_rolling_month_window(data_all, flag, dt_sql, sd):
    flag_arr = pd.Series(np.zeros((len(data_all))))
    
    dt_months = dt_sql.dt.month.values
    deltas = np.diff(dt_months)
    gaps = np.append(-1, np.flatnonzero(deltas == 1)) # spits out any gaps > one month. -1 is for loop below to provide index 0 at start
    
    for i in range(len(gaps)):
        if i < len(gaps)-1: # for all indices except last [i]
            idx = [gaps[i]+1,gaps[i+1]]            
        else: # for last index [i]
            idx = [gaps[i]+1,len(dt_months)-1]
            
        data_mth = data_all[np.arange(idx[0],idx[1])] # all data from month [i] with index matching bigger array
        outliers = data_mth[data_mth > data_mth.mean() + sd*(data_mth.std())] # all outliers in this month matching index of bigger array

        data_all[outliers.index] = np.nan 
        flag_arr[outliers.index] = flag
      
    return data_all, flag_arr

#%% Interpolate qaqced wx station data
def interpolate_qaqc(data_all, data_subset, flag, max_hours):
    flag_arr = pd.Series(np.zeros((len(data_all))))
    mask = data_subset.isna()
    mask = (mask.groupby((mask != mask.shift()).cumsum()).transform(lambda x: len(x) > max_hours)* mask)

    idx = data_subset[np.logical_or(mask == 0, data_subset == np.nan)].index
    interpolated = data_subset.interpolate() # interpolate all nans
    data_all[idx] = interpolated[idx] # place newly interpolated values into the master array
    flag_arr[idx] = flag        

    return data_all, flag_arr

#%%


















