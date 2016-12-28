# -*- coding: utf-8 -*-
#!/usr/bin/env python


import pandas as pd
import json
import requests
import datetime
import os
import boto
from boto.s3.key import Key



def upload_s3(string_passed):
	bucket = ###### 'ENTER YOUR S3 bucket name here' 
	s3 = boto.connect_s3()
	aabucket = s3.get_bucket(bucket,validate='True')
	k = Key(aabucket)
	k.key = datetime.datetime.now().strftime("%Y%m%d-%H%M%S")+str('.json') #filename
	k.set_contents_from_string(string_passed) #send data to s3 bucket
	print("S3 UPLOAD SUCCESSFUL!")


def get_dat():
	#api key
	api_key = ########'ENTER YOUR APP ANNIE API HERE'

	# Creates pandas dataframe that will store data to be exported to JSON for S3 storage
	columns = ['record_id', 'acct_id', 'acct_name', 'platform', 'country_code', 'prod_id', 'prod_name', 
		'record_date', 'iap_units', 'iap_revenue', 'downloads']
	app_annie_list = []



	################################################
	############ get account connections ############
	################################################
	url1 = 'https://api.appannie.com/v1.2/accounts?page_index=0'
	response1 = requests.get(url1, headers= {'Authorization':'bearer {}'.format(api_key)})
	conn_df = pd.read_json(response1.text)

	# store account information (dictionarys) into an array
	accts = []
	prod_list = []
	sales = []
	sales_all = []
	d = {}

	for i in range(len(conn_df)):
		
		if str(conn_df.iloc[i][0].values()[1]) == 'apps':
			accts.append(conn_df.iloc[i][0])

			################################################
			###   get product list for each account    #####
			################################################
			#urls to get product list for each account
			url_prod = 'https://api.appannie.com/v1.2/accounts/{}/products?page_index=0'.format(accts[len(accts)-1].values()[0])
			response_prod = requests.get(url_prod, headers= {'Authorization':'bearer {}'.format(api_key)})
			# store each product id
			prod_list.append(pd.read_json(response_prod.text))

			#create prodcut dictionary; Key: acct_id + prod_id = Value: prod_name
			for k in range(len(prod_list[len(accts)-1])):
				if str(accts[len(accts)-1].values()[7]) == 'amazon-appstore':
					d[str(accts[len(accts)-1].values()[0])+str(prod_list[len(accts)-1].iloc[k][5].values()[1])] = str(prod_list[len(accts)-1].iloc[k][5].values()[4])
				else:
					d[str(accts[len(accts)-1].values()[0])+str(prod_list[len(accts)-1].iloc[k][5].values()[2])] = str(prod_list[len(accts)-1].iloc[k][5].values()[6])
			
			now = datetime.datetime.now()
			end_date = str(now.year)+"-"+str(now.month)+"-"+str(now.day-2)
			start_date = str(prod_list[0].iloc[2][5].values()[5]) # app ios start date

			################################################
			### acct connection sales  #####################
			################################################
			url_sales = 'https://api.appannie.com/v1.2/accounts/{}/sales?break_down=product+country+date'.format(accts[len(accts)-1].values()[0])
			url_sales_all = 'https://api.appannie.com/v1.2/accounts/{}/sales?break_down=product+date'.format(accts[len(accts)-1].values()[0])
			#filters = {'start_date': start_date, 'end_date': end_date, 'currency': 'USD', 'page_index': '0'}
			filters = {'start_date': start_date, 'end_date': end_date, 'currency': 'USD', 'page_index': '0'}
			response_sales = requests.get(url_sales, params = filters, headers= {'Authorization':'bearer {}'.format(api_key)})
			response_sales_all = requests.get(url_sales_all, params = filters, headers= {'Authorization':'bearer {}'.format(api_key)})
			sales.append(pd.read_json(response_sales.text))
			sales_all.append(pd.read_json(response_sales_all.text))
			#print (len(sales[len(accts)-1]))


			if str(accts[len(accts)-1].values()[7]) == 'ios':
				for j in range(len(sales[len(accts)-1])):
					#print float(sales[len(accts)-1].iloc[j][7].values()[4].values()[1].values()[1]) 
					app_annie_list.append([
						str(accts[len(accts)-1].values()[0]) 
							+ str(sales[len(accts)-1].iloc[j][7].values()[3]) 
							+ str(sales[len(accts)-1].iloc[j][7].values()[1])
							+ '{0}{1}{2}'.format(
									datetime.datetime.strptime(str(sales[len(accts)-1].iloc[j][7].values()[0]), '%Y-%m-%d').year, 
									datetime.datetime.strptime(str(sales[len(accts)-1].iloc[j][7].values()[0]), '%Y-%m-%d').month, 
									datetime.datetime.strptime(str(sales[len(accts)-1].iloc[j][7].values()[0]), '%Y-%m-%d').day),  # record_id
						str(accts[len(accts)-1].values()[0]), #acct_id x
						str(accts[len(accts)-1].values()[5]), # acct_name x
						str(accts[len(accts)-1].values()[7]), # platform x
						str(sales[len(accts)-1].iloc[j][7].values()[1]), # country_code x
						str(sales[len(accts)-1].iloc[j][7].values()[3]), # prod_id x
						str(d[ str(accts[len(accts)-1].values()[0]) + str(sales[len(accts)-1].iloc[j][7].values()[3]) ]), #prod_name
						str(sales[len(accts)-1].iloc[j][7].values()[0]), # date x
						int(sales[len(accts)-1].iloc[j][7].values()[2].values()[1].values()[1]), # iap_units x
						float(sales[len(accts)-1].iloc[j][7].values()[4].values()[1].values()[1]),#iap_reveniue
						int(sales[len(accts)-1].iloc[j][7].values()[2].values()[0].values()[1]) #downloads
						])
				for j in range(len(sales_all[len(accts)-1])):
					app_annie_list.append([
						str(accts[len(accts)-1].values()[0]) 
							+ str(sales_all[len(accts)-1].iloc[j][7].values()[3]) 
							+ str(sales_all[len(accts)-1].iloc[j][7].values()[1])
							+ '{0}{1}{2}'.format(
									datetime.datetime.strptime(str(sales_all[len(accts)-1].iloc[j][7].values()[0]), '%Y-%m-%d').year, 
									datetime.datetime.strptime(str(sales_all[len(accts)-1].iloc[j][7].values()[0]), '%Y-%m-%d').month, 
									datetime.datetime.strptime(str(sales_all[len(accts)-1].iloc[j][7].values()[0]), '%Y-%m-%d').day),  # record_id
						str(accts[len(accts)-1].values()[0]), #acct_id x
						str(accts[len(accts)-1].values()[5]), # acct_name x
						str(accts[len(accts)-1].values()[7]), # platform x
						str(sales_all[len(accts)-1].iloc[j][7].values()[1]), # country_code x
						str(sales_all[len(accts)-1].iloc[j][7].values()[3]), # prod_id x
						str(d[ str(accts[len(accts)-1].values()[0]) + str(sales_all[len(accts)-1].iloc[j][7].values()[3]) ]), #prod_name
						str(sales_all[len(accts)-1].iloc[j][7].values()[0]), # date x
						int(sales_all[len(accts)-1].iloc[j][7].values()[2].values()[1].values()[1]), # iap_units x
						float(sales_all[len(accts)-1].iloc[j][7].values()[4].values()[1].values()[1]),#iap_reveniue
						int(sales_all[len(accts)-1].iloc[j][7].values()[2].values()[0].values()[1]) #downloads
						])				


			else:
				for j in range(len(sales[len(accts)-1])):
					app_annie_list.append([
						str(accts[len(accts)-1].values()[0]) 
							+ str(sales[len(accts)-1].iloc[j][7].values()[3]) 
							+ '{0}{1}{2}'.format(
									datetime.datetime.strptime(str(sales[len(accts)-1].iloc[j][7].values()[0]), '%Y-%m-%d').year, 
									datetime.datetime.strptime(str(sales[len(accts)-1].iloc[j][7].values()[0]), '%Y-%m-%d').month, 
									datetime.datetime.strptime(str(sales[len(accts)-1].iloc[j][7].values()[0]), '%Y-%m-%d').day), #record_id
						str(accts[len(accts)-1].values()[0]), #acct_id x
						str(accts[len(accts)-1].values()[6]), # acct_name x
						str(accts[len(accts)-1].values()[7]), # platform x
						str(sales[len(accts)-1].iloc[j][7].values()[1]), # country_code x
						str(sales[len(accts)-1].iloc[j][7].values()[3]), # prod_id x
						str(d[ str(accts[len(accts)-1].values()[0]) + str(sales[len(accts)-1].iloc[j][7].values()[3]) ]), #prod_name
						str(sales[len(accts)-1].iloc[j][7].values()[0]), # date x
						int(sales[len(accts)-1].iloc[j][7].values()[2].values()[1].values()[0]), # iap_units x
						float(sales[len(accts)-1].iloc[j][7].values()[4].values()[1].values()[0]),#iap_reveniue
						int(sales[len(accts)-1].iloc[j][7].values()[2].values()[0].values()[0]) #downloads
						])
				for j in range(len(sales_all[len(accts)-1])):
					app_annie_list.append([
						str(accts[len(accts)-1].values()[0]) 
							+ str(sales_all[len(accts)-1].iloc[j][7].values()[3]) 
							+ str(sales_all[len(accts)-1].iloc[j][7].values()[1])
							+ '{0}{1}{2}'.format(
									datetime.datetime.strptime(str(sales_all[len(accts)-1].iloc[j][7].values()[0]), '%Y-%m-%d').year, 
									datetime.datetime.strptime(str(sales_all[len(accts)-1].iloc[j][7].values()[0]), '%Y-%m-%d').month, 
									datetime.datetime.strptime(str(sales_all[len(accts)-1].iloc[j][7].values()[0]), '%Y-%m-%d').day), #record_id
						str(accts[len(accts)-1].values()[0]), #acct_id x
						str(accts[len(accts)-1].values()[6]), # acct_name x
						str(accts[len(accts)-1].values()[7]), # platform x
						str(sales_all[len(accts)-1].iloc[j][7].values()[1]), # country_code x
						str(sales_all[len(accts)-1].iloc[j][7].values()[3]), # prod_id x
						str(d[ str(accts[len(accts)-1].values()[0]) + str(sales_all[len(accts)-1].iloc[j][7].values()[3]) ]), #prod_name
						str(sales_all[len(accts)-1].iloc[j][7].values()[0]), # date x
						int(sales_all[len(accts)-1].iloc[j][7].values()[2].values()[1].values()[0]), # iap_units x
						float(sales_all[len(accts)-1].iloc[j][7].values()[4].values()[1].values()[0]),#iap_reveniue
						int(sales_all[len(accts)-1].iloc[j][7].values()[2].values()[0].values()[0]) #downloads
						])
				



	app_annie_df = pd.DataFrame(app_annie_list, columns=columns)
	#lename = datetime.datetime.now().strftime("%Y%m%d-%H%M%S")
	#with open("/Users/robert/Documents/aa_json/20130214-20161128.json", "w+") as output_file:
	#with open("/Users/cowabungadood/Documents/aa_json/20130214-20161128.json", "w+") as output_file:
	#with open(os.path.join(os.path.expanduser('~'), str("appannie/appannie_jsons")+str(filename)+str(".json")), "w+") as output_file:
	#	output_file.write(app_annie_df.to_json(orient='records'))
	#app_annie_df.set_index(['record_id'], inplace=True)
	#app_annie_df.to_csv("/Users/cowabungadood/Documents/aa_json/appdat.csv")
	print ("JSON DOWNLOAD SUCCESSFUL!")
	x = app_annie_df.to_json(orient='records')[1:-1].replace('},{','} {')
	upload_s3(x)






if __name__ == '__main__':
	get_dat()





		
