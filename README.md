This is a first iteration of a script utilizing the AppAnnie API, AWS, and AirFlow.  APP ANNIE API calls are made using the python requests package.  Connection to AWS S3 is made by using python boto package.  Redshift conneciton is made using python psycopg2 package.  This application is used to grab data from the AppAnnie API and save it to an S3 bucket, then from the S3 bucket connect to REDSHIFT an execite an Upsert.