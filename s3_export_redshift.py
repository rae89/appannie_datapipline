#!/usr/bin/env python
import psycopg2
import boto

connection_string = """dbname='events'
port= 'ENTER REDShIFT PORT NUMBER'
user='ENTER REDSHIFT USERNAME'
password='ENTER REDSHIFT PASSWORD'
host='ENTER REDHSIFT HOST STRING'"""
conn = psycopg2.connect(connection_string)
cursor = conn.cursor()

def get_s3object(bucketname):
	s3 = boto.connect_s3()
	bucket = s3.get_bucket(bucketname)
	#bucket = s3.get_bucket('nixhydra-appannie')
	bucketList = bucket.list()
	orderedList = sorted(bucketList, key=lambda k: k.last_modified)
	o = str(orderedList[-1].name) #last updated object
	return o
def create_staging(stage, liketable):
	command = "create temp table if not exists {} (like {});".format(stage, liketable)
	cursor.execute(command)
	conn.commit()

def copy_s3toRed(table, bucket, obj, role):
	command = """COPY {} from 's3://{}/{}' 
		with credentials 'aws_iam_role={}'
		format as json 'auto';""".format(table, bucket, obj, role)
	cursor.execute(command)
	conn.commit()

def merge_data(target, stage):
	command = """begin transaction;
	delete from {} as aa
	using {}
	where aa.record_id = {}.record_id;
	insert into {}
	select * from {};
	end transaction;""".format(target, stage)
	cursor.execute(command)
	conn.commit()
def drop_staging(stage):
	command = """drop table if exists {};""".format(stage)
	cursor.execute(command)
	conn.commit()


if __name__ == '__main__':
	create_staging('STAGE TABLE NAME','TARGET TABLE NAME')
	last_sorted_obj = get_s3object('S3 BUCKET NAME')
	copy_s3toRed('STAGE TABLE NAME', 'S3 BUCKET NAME', last_sorted_obj,'AWS IAM ROLE')
	merge_data('TARGET TABLE NAME', 'STAGE TABLE NAME')
	drop_staging('STAGE TABLE NAME')
