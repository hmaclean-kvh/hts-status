#!/usr/bin/python
#KVH HTS-feed program
#Hayden S. Maclean -> hmaclean@kvh.com

from datetime import date, timedelta
import datetime
from struct import *
import requests
from pygelf import GelfUdpHandler
import logging
import subprocess
import os
import pickle

def send_hb():
  hb_config = {}
  hb_config['origin'] = "HTS Terminal Status Processor"
  hb_config['timeout'] = 60
  hb_config['tags'] = ["Mclean,Docker,dev_hts_bot"]
  r = requests.post("http://192.168.220.35:8080/api/heartbeat",json=hb_config,timeout=10)



#bucket_list =["s3://kvh-hts-terminal-status/terminal","s3://kvh-hts-terminal-status/sspc"]
bucket_list =["s3://kvh-hts-terminal-status/terminal"]

# Logging
logging.basicConfig(filename='/root/hts_status.log',format='%(asctime)s %(levelname)s %(message)s',level=logging.WARNING)


terminal_status_cache = {}

# list of metrics expected as strings
str_value_key = ['term_state','term_satellite_id','term_inroute_group_id','term_inet_id','term_beam_id','nominal_carrier','term_sas_id','term_service_area_obj_id', 'term_channel_id']

# Statement list
statement_list = []

def getlatestfilefromS3(path):
	output = runS3Query('s3cmd ls '+path)

	fmt_output = str(output).split()
	file_list = []
	latest_file_in_s3 = fmt_output[-1]
	latest_ts_in_s3 = fmt_output[-3]

	logging.info('Latest file in s3: {} @ {}'.format(latest_file_in_s3, latest_ts_in_s3))

	# Get last file uplaoded to the bucket
	getfileresult = runS3Query('s3cmd get '+ latest_file_in_s3+ ' /root/hts-status/')
	if '100%' in getfileresult:
		logging.info('s3cmd get: Success!')
	else:
		logging.warning('s3cmd get: Failure!')

	file_list.append(latest_file_in_s3.split('/')[-1].replace('.gz',''))

	# If there are more than one file uplaoded at the same time.
	if fmt_output.count(latest_ts_in_s3) > 1:
		logging.info('More then one file with the same timestamp!')
		logging.info('Second latest file in s3: {} @ {}'.format(fmt_output[-5], fmt_output[-7]))
		getfileresult = runS3Query('s3cmd get '+ fmt_output[-5]+ ' /root/hts-status/')
		if '100%' in getfileresult:
			logging.info('s3cmd get: Success!')
		else:
			logging.warning('s3cmd get: Failure!')

		file_list.append(fmt_output[-5].split('/')[-1].replace('.gz',''))

	os.system('gzip -d /root/hts-status/*.gz')
	return file_list

def parse_terminal_file(file):
	# Parse file it will be in the same directory as the source code
	with open(file, 'r') as csvfile:
		for line in csvfile:
			line_list = line.replace('"','').replace(' ','').strip('\n\r').split(',')

			terminal_id = line_list[0]
			if len(str(terminal_id)) == 8:
				measurement_name = line_list[1]
				if any(st in line_list[1] for st in str_value_key):
					measurement_value = '"'+line_list[2]+'"'
				else:
					measurement_value = line_list[2]

				ts =  line_list[3]

				add_statement('{},terminal_id={} value={} {}\n'.format(measurement_name,terminal_id,measurement_value,ts))

				update_terminal_status_cache(terminal_id,measurement_name,measurement_value,ts)
			else:
				logging.info('Malformed terminal_id found: {}'.format(terminal_id))

def parse_sspc_file(file):
	# Parse file it will be in the same directory as the source code
	with open(file, 'r') as csvfile:
		for line in csvfile:
			line_list = line.replace('"','').replace(' ','').strip('\n\r').split(',')

			terminal_id = line_list[0].split(':')[0].split('-')[-1]
			if len(str(terminal_id)) == 8:
				sspc_name = line_list[0].split(':')[1]
				measurement_name = line_list[1]
				if measurement_name == 'sspc_gsp' and sspc_name == 'SSPP1-KVH_Mgmt_Net':
					measurement_value = '"'+line_list[2]+'"'
				#else:
				#	measurement_value = line_list[2]

					ts =  line_list[3]

					add_statement('{},terminal_id={},sspc={} value={} {}\n'.format(measurement_name,terminal_id,sspc_name,measurement_value,ts))

					update_terminal_status_cache(terminal_id,sspc_name+measurement_name,measurement_value,ts)

			else:
				logging.info('Malformed terminal_id found: {}'.format(terminal_id))

def add_statement(statement):
	statement_list.append(statement)

def update_terminal_status_cache(terminal_id,measurement_name,measurement_value,ts):
	temp = {measurement_name: (measurement_value.replace('"',''), ts)}
	if terminal_id in terminal_status_cache:
		if measurement_name in terminal_status_cache[terminal_id]:
			# Measurement name exists check value
			#if terminal_status_cache[terminal_id][measurement_name][0] != measurement_value.replace('"',''):
			terminal_status_cache[terminal_id].update(temp)
			logging.debug('status_cache: UpdateM tid: {} m_name: {} m_value: {} ts: {}'.format(terminal_id,measurement_name,measurement_value,ts))
		else:
			# Measurement Name does not exist
			terminal_status_cache[terminal_id].update(temp)
			logging.debug('status_cache: NewM tid: {} m_name: {} m_value: {} ts: {}'.format(terminal_id,measurement_name,measurement_value,ts))
	else:
		terminal_status_cache[terminal_id] = temp
		logging.info('status_cache: New tid: {}'.format(terminal_id))

def commit_terminal_status_cache(terminal_status_cache):
	try:
		with open('/root/terminal_status_cache.pkl', 'wb') as output:
			pickle.dump(terminal_status_cache, output)
	except Exception as ex:
		logging.critical('Encountered Unknown exception: {}'.format(str(ex)))


def load_terminal_status_cache():
	try:
		with open('/root/terminal_status_cache.pkl', 'rb') as input:
			return pickle.load(input)
	except Exception as ex:
		logging.warning('terminal_status_cache not found or failed to load {}'.format(str(ex)))
		term_status_cache = {}
		return term_status_cache

def send_data(Data):
	influxpost = requests.post("http://influx.ops.kvh.com:8086/write?db=HTS_DEV&precision=s", data=Data)
	if influxpost.status_code != 204:
		logging.warning('Influx Status Code: {}'.format(influxpost.status_code))
		logging.warning('Influx Post Text: {}'.format(influxpost.text))
	else:
		logging.info('Influx Status Code: {}'.format(influxpost.status_code))

        influxpost = requests.post("https://192.168.15.49/write?db=HTS_DEV&precision=s", data=Data,verify=False,auth=('administrator','HealthMonitor!'))
        if influxpost.status_code != 204:
                logging.warning('Influx Status Code: {}'.format(influxpost.status_code))
                logging.warning('Influx Post Text: {}'.format(influxpost.text))
        else:
                logging.info('Influx Status Code: {}'.format(influxpost.status_code))




def runS3Query(cmd):

	return subprocess.check_output(cmd, shell=True)


if __name__ == "__main__":
	try:
		os.system('rm /root/hts-status/*.csv*')

		datepath = datetime.datetime.now().strftime('/%Y/%m/%d/')

		# Load memcache
		terminal_status_cache = load_terminal_status_cache()
		file_list = []

		for bucket in bucket_list:
			file_list += getlatestfilefromS3(bucket+datepath)

		logging.info('Files to be processed: {}'.format(file_list))

		for file in file_list:
			logging.debug('File being processed: {}'.format(file))
			if 'terminal' in file:
				parse_terminal_file('/root/hts-status/'+file)
#			elif 'sspc' in file:
#				parse_sspc_file('/root/hts-status/'+file)
#			else:
#				logging.warning('Unknown File Type... Not processed')
		payload = "".join(statement_list)

		#print payload
		#print terminal_status_cache
		send_data(payload)
		send_hb()

		# Commit memcache
		commit_terminal_status_cache(terminal_status_cache)
	except Exception as ex:
		logging.critical('Encountered Unknown exception: {}'.format(str(ex)))
		os.system('rm /root/hts-status/*.csv')
