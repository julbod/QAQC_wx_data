#%% This code creates an SQL database for each qaqc wx station databases
# all databases are by default filled with nans
# Written by Julien Bodart (VIU) - 12/13/2023

import os
import pandas as pd 
from sqlalchemy import create_engine
import numpy as np
import re

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
wx_stations = [w.replace('clean_eastbuxton_archive', 'clean_temp') for w in wx_stations] # rename steph3 so it doesn't get cut out
wx_stations = [x for x in wx_stations if not "clean_eastbuxton" in x] # remove machmell from list
wx_stations = [w.replace('clean_temp', 'clean_eastbuxton') for w in wx_stations] # rename steph3 so it doesn't get cut out
wx_stations = [x for x in wx_stations if not "steph" in x] # remove all stephanies
wx_stations = [w.replace('clean_machmellkliniklini', 'clean_Machmellkliniklini') for w in wx_stations] # rename machmellkliniklini so it doesn't get cut out
wx_stations = [x for x in wx_stations if not "machmell" in x] # remove machmell from list
wx_stations = [x for x in wx_stations if not "russell" in x] # remove russell from list
wx_stations = [x for x in wx_stations if not "rennellpass" in x] # remove rennell from list
wx_stations = [x for x in wx_stations if not "plummerhut" in x] # remove plummer from list
wx_stations = [w.replace('clean_Stephanie3', 'clean_steph3') for w in wx_stations] # rename steph3 back to original
wx_stations = [w.replace('clean_Machmellkliniklini', 'clean_machmellkliniklini') for w in wx_stations] # rename machmellkliniklini back to original
wx_stations_name = list(map(lambda st: str.replace(st, 'clean_', ''), wx_stations)) # remove 'clean_' for csv export
wx_stations_name_cap = [wx_name.capitalize() for wx_name in wx_stations_name] # capitalise station name

#%% Loop over each station at a time and clean up the snow depth variable
for l in range(len(wx_stations_name)):
    sql_database = wx_stations_name[l]
    sql_name = wx_stations_name_cap[l]
    print('###### Creating dummy sql database for station: %s ######' %(sql_name))     
    
    #%% import current data on SQL database and clean name of some columns to match
    # CSV column names
    sql_file = pd.read_sql(sql="SELECT * FROM clean_" + sql_database, con = engine)
    sql_file = sql_file.set_index('DateTime').asfreq('1H').reset_index()
    
    #%% only keep data from oldest to last water year
    new_df = sql_file[:int(np.flatnonzero(sql_file['DateTime'] == '2023-10-01 00:00:00'))]
    nanout = [c for c in new_df.columns if c not in ['DateTime', 'WatYr']]
    new_df[nanout] = np.nan
    
    # add flags columns
    colname = nanout
    colname_flags = [direction + '_flags' for direction in colname]
    
    # merge both dataframes together
    new_df[colname_flags] = np.nan
    
    # sort columns by alphabetical names
    colname_new = new_df.columns[2:]
    temp_df = new_df[colname_new]
    temp_df = temp_df[sorted(temp_df.columns)]
    
    # merge into final dataframe
    df_full = pd.concat([new_df['DateTime'], new_df['WatYr'],temp_df],axis=1)
    
    # import database to SQL
    df_full.to_sql(name='qaqc_%s' %wx_stations_name[l], con=engine, if_exists = 'fail', index=False)
    
    #%% alternatively, create sql file that can be manually imported into 
    # phpMyAdmin (started process but above is much more efficient)
    # table_name = 'qaqc_' + wx_stations_name[l]
    
    # sql_file_text = '''/*!40101 SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT */;
    # /*!40101 SET @OLD_CHARACTER_SET_RESULTS=@@CHARACTER_SET_RESULTS */;
    # /*!40101 SET @OLD_COLLATION_CONNECTION=@@COLLATION_CONNECTION */;
    # /*!40101 SET NAMES utf8mb4 */;
    
    # --
    # -- Database: `viuhydro_wx_data_v2`
    # --
    
    # -- --------------------------------------------------------
    
    # --
    # -- Table structure for table `%s`
    # --
    
    # CREATE TABLE : '%s' (
    #   `DateTime` datetime NOT NULL,
    #   `WatYr` int(11) NOT NULL,
    #   `Air_Temp` float DEFAULT NULL,
    #   `RH` float DEFAULT NULL,
    #   `BP` float DEFAULT NULL,
    #   `Wind_Speed` float DEFAULT NULL,
    #   `Wind_Dir` float DEFAULT NULL,
    #   `Pk_Wind_Speed` float DEFAULT NULL,
    #   `Pk_Wind_Dir` float DEFAULT NULL,
    #   `PC_Tipper` float DEFAULT NULL,
    #   `PP_Tipper` float DEFAULT NULL,
    #   `PC_Raw_Pipe` float DEFAULT NULL,
    #   `PP_Pipe` float DEFAULT NULL,
    #   `Snow_Depth` float DEFAULT NULL,
    #   `SWE` float DEFAULT NULL,
    #   `Solar_Rad` float DEFAULT NULL,
    #   `SWU` float DEFAULT NULL,
    #   `SWL` float DEFAULT NULL,
    #   `LWU` float DEFAULT NULL,
    #   `LWL` float DEFAULT NULL,
    #   `Lysimeter` float DEFAULT NULL,
    #   `Soil_Moisture` float DEFAULT NULL,
    #   `Soil_Temperature` float NOT NULL,
    #   `Batt` float DEFAULT NULL
    # ) ENGINE=InnoDB DEFAULT CHARSET=latin1 COLLATE=latin1_swedish_ci;

    # --
    # -- Dumping data for table `qaqc_apelake`
    # --
    
    # INSERT INTO `%s` (`DateTime`, `WatYr`, `Air_Temp`, `RH`, `BP`, `Wind_Speed`, `Wind_Dir`, `Pk_Wind_Speed`, `Pk_Wind_Dir`, `PC_Tipper`, `PP_Tipper`, `PC_Raw_Pipe`, `PP_Pipe`, `Snow_Depth`, `SWE`, `Solar_Rad`, `SWU`, `SWL`, `LWU`, `LWL`, `Lysimeter`, `Soil_Moisture`, `Soil_Temperature`, `Batt`) VALUES
    # (''' %(table_name,table_name,table_name)
    #%%