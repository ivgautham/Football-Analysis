from setup import psycopg2

#Obtaining the connection to RedShift
con=psycopg2.connect(dbname= 'football-analytics', host='football-analysis.cg00zqictr2g.ap-south-1.redshift.amazonaws.com:5439', port= '5439', user= 'awsuser', password= 'OGUSUdwrp436.)')


#Copy Command as Variable
copy_command="""copy  competitions (competition_id,season_id,country_name,competition_name,competition_gender,competition_youth,competition_international,season_name,match_updated,match_updated_360,match_available_360,match_available)
from 's3://football-analysis-s3/competitions.json' 
iam_role 'arn:aws:iam::992382570063:role/'
IGNOREHEADER 1;"""

#Opening a cursor and run copy query
cur = con.cursor()
cur.execute("truncate table competitions;")
cur.execute(copy_command)
con.commit()

#Close the cursor and the connection
cur.close()
con.close()

print ("Print finised executing copy command")