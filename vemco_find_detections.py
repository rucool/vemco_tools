from sys import argv, exit
script, full_detections_file, output_dir = argv

import pandas as pd
import numpy as np
import os

vemcoFile=os.path.splitext(os.path.split(full_detections_file)[1])[0]

vemco=pd.read_csv(full_detections_file,header=None)
vemco.columns=['Receiver','line','time_orig','id1','id2','na2','na3','na4','na5','na6','na7','na8','na9','na10']
ind=vemco['id1']!='STS'
vemco_sub=vemco[ind]
vemco_sub['time']=pd.to_datetime(vemco_sub['time_orig'],format='%Y-%m-%d %H:%M:%S')
vemco_sub['Date and Time (UTC)']=vemco_sub['time'].dt.strftime('%m/%d/%Y %H:%M:%S')
vemco_sub['Receiver']='RXLive-'+vemco_sub['Receiver'].astype(str)
vemco_sub['Transmitter']=vemco_sub['id1']+'-'+vemco_sub['id2']
vemco_sub=vemco_sub[['Date and Time (UTC)','Receiver','Transmitter']]
vemco_sub['Transmitter Name']=np.nan
vemco_sub['Transmitter Serial']=np.nan
vemco_sub['Sensor Value']=np.nan
vemco_sub['Sensor Unit']=np.nan
vemco_sub['Station Name']=np.nan
vemco_sub['Latitude']=np.nan
vemco_sub['Longitude']=np.nan
vemco_sub.to_csv(os.path.join(output_dir,vemcoFile+'-detectionsonly.csv'),index=False)

exit()