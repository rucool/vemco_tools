import os
from erddapy import ERDDAP
import pandas as pd
import numpy as np
import warnings
import glob
import subprocess
import requests
import sys
import argparse

warnings.simplefilter("ignore")

def main(args):
    status = 0

    deployment = args.deployment[0]
    receivers = args.receiver.replace(' ','').split(',')
    directory = args.directory 
    vmtfile = args.vmt

    receiver_types = ['rxlive', 'vmt']
    vmtinfo = pd.read_csv(vmtfile)

    glider_api = 'https://marine.rutgers.edu/cool/data/gliders/api/'
    deployment_api = requests.get(f'{glider_api}deployments/?deployment={deployment}').json()['data'][0]
    startT = pd.to_datetime(deployment_api['start_date_epoch'], unit='s')
    endT = pd.to_datetime(deployment_api['end_date_epoch'], unit='s')

    deploymentT = deployment.split('-')[-1]
    deploymentY = deploymentT[:4]
    glider = '-'.join(deployment.split('-')[:-1])
    traj = True
    missionid = glider.upper() + pd.to_datetime(deploymentT).strftime('%Y%m%d%H')

    deployment_info = {'PLATFORM_ID': glider.upper(), 'OTN_MISSION_ID': missionid, 
                    'DEPLOY_DATE_TIME': startT.strftime('%Y-%m-%dT%H:%M:%S'), 
                    'DEPLOY_LAT': None, 'DEPLOY_LONG': None, 'DEPLOYED_BY': None, 
                    'RECOVER_DATE_TIME': endT.strftime('%Y-%m-%dT%H:%M:%S'), 
                    'RECOVER_LAT': None, 'RECOVER_LONG': None}

    deployment_dir = os.path.join(directory,'deployments', deploymentY, deployment)

    if not os.path.isdir(deployment_dir):
        print(f'{deployment_dir} does not exist. Check deployment name. Exiting without processing.')
        return 1

    ru_erddap = ERDDAP(server='http://slocum-data.marine.rutgers.edu/erddap', protocol='tabledap')

    glider_url = ru_erddap.get_search_url(search_for=f'{deployment}-trajectory', response='csv')
    try:
        glider_datasets = pd.read_csv(glider_url)['Dataset ID']   
    except:
        print(f'{deployment} not found on ERDDAP. Continuing without trajectory file generation and metadata grab.')
        traj=False

    if traj:
        ru_erddap.dataset_id = glider_datasets[0]
        all_info = pd.read_csv(ru_erddap.get_info_url(response='csv'))
        ru_erddap.variables = ['time', 'longitude', 'latitude']
        ru_erddap.constraints = {'m_gps_lat<': 10000}
        glider_traj = ru_erddap.to_pandas(distinct=True)
        glider_traj['time'] = pd.to_datetime(glider_traj['time (UTC)']) #, format='%Y-%m-%dT%H:%M:%S.%fZ')
        glider_traj = glider_traj.sort_values(by='time', ignore_index=True)
        glider_traj['GLIDER_ID'] = glider.upper()
        ru_erddap.variables = ['time', 'longitude', 'latitude']
        ru_erddap.constraints = {'m_gps_lat<': 10000, 'time<': (min(glider_traj['time'])+pd.Timedelta(hours=.5)).strftime('%Y-%m-%dT%H:%M')}
        glider_start = ru_erddap.to_pandas()
        ru_erddap.variables = ['time', 'longitude', 'latitude']
        ru_erddap.constraints = {'m_gps_lat<': 10000, 'time>': (max(glider_traj['time'])-pd.Timedelta(hours=.5)).strftime('%Y-%m-%dT%H:%M')}
        glider_end = ru_erddap.to_pandas()
        deployment_info['DEPLOY_LAT'] = str(round(np.nanmedian(glider_start['latitude (degrees_north)']),3))
        deployment_info['DEPLOY_LONG'] = str(round(np.nanmedian(glider_start['longitude (degrees_east)']),3))
        deployment_info['RECOVER_LAT'] = str(round(np.nanmedian(glider_end['latitude (degrees_north)']),3))
        deployment_info['RECOVER_LONG'] = str(round(np.nanmedian(glider_end['longitude (degrees_east)']),3))
        contributors = all_info['Value'][np.logical_and(all_info['Variable Name']=='NC_GLOBAL', all_info['Attribute Name']=='contributor_name')].values[0].split(',')
        roles = all_info['Value'][np.logical_and(all_info['Variable Name']=='NC_GLOBAL', all_info['Attribute Name']=='contributor_role')].values[0].split(',')
        pilots = []
        for i in range(len(roles)):
            if 'pilot' in roles[i].lower():
                pilots = np.append(pilots, contributors[i])
        deployment_info['DEPLOYED_BY'] = ', '.join(pilots)
        glider_traj = glider_traj.drop(columns='time')

    for i in deployment_info.keys():
        if not deployment_info[i]:
            deployment_info[i] = 'Not found, dig around.'

    if receivers[0]=='default' and not traj:
        print(f'{deployment} not found on ERDDAP. Cannot identify receiver types. Exiting without processing.')
        return 1

    if receivers[0]=='default':
        receivers=[]
        for i in receiver_types:
            if f'instrument_{i}' in list(all_info['Variable Name']):
                receivers=np.append(receivers,i)
        receivers=list(receivers)

    for rec in receivers:
        instrument_info = {'INS_MODEL_NO': None, 'INS_SERIAL_NO': None, 
                        'TRANSMITTER': None, 'TRANSMIT_MODEL': None,
                        'DOWNLOAD_DATE_TIME': None,
                        'FILENAME': None, 'COMMENTS': None}
        if len(receivers)>1:
            mission_add = chr(receivers.index(rec)+97)
        else:
            mission_add = ''

        data_dir = os.path.join(deployment_dir, 'data', rec)

        if not os.path.isdir(data_dir):
            print(f'{data_dir} does not exist. Check that {rec} data directory has been started and data has been placed there, if applicable. Exiting without processing.')
            return 1

        if traj and f'instrument_{rec}' not in list(all_info['Variable Name']):
            print(f'instrument_{rec} is not included in ERDDAP dataset. Will process, but should check metadata and/or data files to confirm they are paired with the correct deployment.')
        else:
            instrument_info['INS_SERIAL_NO'] = all_info['Value'][np.logical_and(all_info['Variable Name']==f'instrument_{rec}', all_info['Attribute Name']=='serial_number')].values[0]

        if not os.path.isdir(os.path.join(data_dir, 'to-matos')):
            os.mkdir(os.path.join(data_dir, 'to-matos'))

        if rec=='rxlive':
            instrument_info['INS_MODEL_NO'] = 'Rx-LIVE'
            instrument_info['DOWNLOAD_DATE_TIME'] = endT.strftime('%Y-%m-%dT%H:%M:%S')
            rxlive = True
            detection_file = os.path.join(data_dir, f'{deployment}-cat.vem')
            if not os.path.exists(detection_file):
                print(f'Full {rec} detection file {detection_file} not found. Continuing without generating detections only file.')
                rxlive = False
            else:
                try:
                    vemco=pd.read_csv(detection_file,header=None)
                except Exception as error:
                    rxlive = False
                    print(f'Error reading {detection_file} printed below. If UnicodeDecodeError, unable to decode: open file, find bad character position using :goto POSITION_NUMBER, delete line, and rerun. Continuing processing without generation of detection file.\n\n')
                    print(error)
                    print('\n\n')

            if rxlive:
                vemco.columns = ['Receiver','line','time_orig','id1','id2','na2','na3','na4','na5','na6','na7','na8','na9','na10']
                ind = vemco['id1']!='STS'
                vemco_sub = vemco[ind]
                vemco_sub['time'] = pd.to_datetime(vemco_sub['time_orig'])
                vemco_sub['Date and Time (UTC)'] = vemco_sub['time'].dt.strftime('%m/%d/%Y %H:%M:%S')
                vemco_sub['Receiver'] = 'RXLive-'+vemco_sub['Receiver'].astype(str)
                vemco_sub['Transmitter'] = vemco_sub['id1']+'-'+vemco_sub['id2']
                vemco_sub = vemco_sub[['Date and Time (UTC)','Receiver','Transmitter']]
                vemco_sub['Transmitter Name'] = np.nan
                vemco_sub['Transmitter Serial'] = np.nan
                vemco_sub['Sensor Value'] = np.nan
                vemco_sub['Sensor Unit'] = np.nan
                vemco_sub['Station Name'] = np.nan
                vemco_sub['Latitude'] = np.nan
                vemco_sub['Longitude'] = np.nan
                vemco_sub['GLIDER_ID'] = glider.upper()
                vemco_sub['MISSION_ID'] = missionid + mission_add
                vemco_sub.to_csv(os.path.join(data_dir, 'to-matos',f'{deployment}-rxlive-detectionsonly.csv'),index=False)
                instrument_info['FILENAME'] = f'{deployment}-rxlive-detectionsonly.csv'

        if rec=='vmt':
            instrument_info['INS_MODEL_NO'] = 'VMT'
            instrument_info['TRANSMIT_MODEL'] = 'VMT'
            if instrument_info['INS_SERIAL_NO']:
                if instrument_info['INS_SERIAL_NO'] in list(vmtinfo['SN'].astype(str)):
                    instrument_info['TRANSMITTER'] = vmtinfo['TransmitterID'][vmtinfo['SN'].astype(str)==instrument_info['INS_SERIAL_NO']].values[0]
                else:
                    print(f'Transmitter ID for VMT SN {instrument_info['INS_SERIAL_NO']} not found in {vmtfile}.')
                    instrument_info['TRANSMITTER'] = 'Not found, dig around.'
            else:
                print(f'VMT serial number and transmitter ID not found.')
                instrument_info['TRANSMITTER'] = 'Not found, dig around.'
                instrument_info['INS_MODEL_NO'] = 'Not found, dig around.'
            vmtfiles = glob.glob(os.path.join(data_dir,'VMT_*.vrl'))
            for f in vmtfiles:
                f0=os.path.basename(f)
                subprocess.call(['cp', os.path.join(f), os.path.join(data_dir, 'to-matos', f0)])
                vmtfiles[vmtfiles.index(f)] = f0
                if not instrument_info['DOWNLOAD_DATE_TIME']:
                    d = os.path.splitext(f0)[0].split('_')[-2]
                    t = os.path.splitext(f0)[0].split('_')[-1]
                    instrument_info['DOWNLOAD_DATE_TIME'] = pd.to_datetime(d+'T'+t).strftime('%Y-%m-%dT%H:%M:%S')
            instrument_info['FILENAME'] = ', '.join(vmtfiles)

        glider_traj['MISSION_ID'] = missionid + mission_add
        glider_traj.to_csv(os.path.join(data_dir, 'to-matos',f'{deployment}{mission_add}-trajectory.csv'),index=False)
        instrument_info['COMMENTS'] = f'glider trajectory file: {deployment}{mission_add}-trajectory.csv (make sure to add any other necessary comments as well)'

        if not instrument_info['INS_SERIAL_NO']:
            instrument_info['INS_MODEL_NO'] = 'Not found, dig around.'

        print(f"\nDeployment Metadata for {deployment} {rec} (if 'None' leave blank, or look up if required/green header):")
        print('\n')
        print(f"PLATFORM_ID: {deployment_info['PLATFORM_ID']}")
        print(f"OTN_MISSION_ID: {missionid + mission_add}")
        print(f"INS_MODEL_NO: {instrument_info['INS_MODEL_NO']}")
        print(f"INS_SERIAL_NO: {instrument_info['INS_SERIAL_NO']}")
        print(f"TRANSMITTER: {instrument_info['TRANSMITTER']}")
        print(f"TRANSMIT_MODEL: {instrument_info['TRANSMIT_MODEL']}")
        print(f"DEPLOY_DATE_TIME: {deployment_info['DEPLOY_DATE_TIME']}")
        print(f"DEPLOY_LAT: {deployment_info['DEPLOY_LAT']}")
        print(f"DEPLOY_LONG: {deployment_info['DEPLOY_LONG']}")
        print('CHECK_COMPLETE_TIME: None')
        print('MEMORY_ERASED_AT_DEPLOY: None')
        print('GLIDER_BATTERY_INSTALL_DATE: None')
        print('GLIDER_EXPECTED_BATTERY_LIFE: None')
        print('GLIDER_VOLTAGE_AT_DEPLOY: None')
        print(f"DEPLOYED_BY: {deployment_info['DEPLOYED_BY']}")
        print(f"RECOVER_DATE_TIME: {deployment_info['RECOVER_DATE_TIME']}")
        print("RECOVERED: 'y' if recovered, 'n' if still deployed, 'l' if lost")
        print(f"RECOVER_LAT: {deployment_info['RECOVER_LAT']}")
        print(f"RECOVER_LONG: {deployment_info['RECOVER_LONG']}")
        print(f"DATA_DOWNLOADED: 'y' if downloaded, 'n' if real-time or not yet downloaded")
        print(f"DOWNLOAD_DATE_TIME: {instrument_info['DOWNLOAD_DATE_TIME']}")
        print('DOWNLOAD_STATUS: None')
        print(f"FILENAME: {instrument_info['FILENAME']}")
        print(f"COMMENTS: {instrument_info['COMMENTS']}")
        print('\n')

    return status


if __name__ == '__main__':
    arg_parser = argparse.ArgumentParser(description=main.__doc__,
                                         formatter_class=argparse.ArgumentDefaultsHelpFormatter)

    arg_parser.add_argument('deployment',
                            nargs='+',
                            help='Glider deployment name formatted as glider-YYYYmmddTHHMM')

    arg_parser.add_argument('-r', '--receiver',
                            help='Receiver type(s) deployed with glider. "default" pulls from metadata on ERDDAP. Other supported options include rxlive, vmt, or both separated by comma with no space.',
                            choices=['default', 'rxlive', 'vmt', 'rxlive,vmt', 'vmt,rxlive'],
                            default='default')

    arg_parser.add_argument('-vmt',
                            default='/Users/nazzaro/Documents/GitHub/vemco_tools/glider_vmt_transmitters.csv',
                            help='File containing VMT SN and Transmitter ID pairs.')

    arg_parser.add_argument('-d', '--directory',
                            help='Upper level directory containing deployment.',
                            default='/Users/nazzaro/Documents/Fisheries/matos/')

    parsed_args = arg_parser.parse_args()

    sys.exit(main(parsed_args))