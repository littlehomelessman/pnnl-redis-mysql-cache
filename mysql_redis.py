import redis
import mysql.connector
import datetime
import yaml

# load yml file to dictionary
credentials = yaml.load(open('./credentials.yaml'), Loader = yaml.BaseLoader)

# hard coded data type
table_names={"fake_zoneairflow_labeled_agg":"airflow", "fake_zonetemp_labeled_agg":"temperature"}

# valid for one day
EXPIRE_TIME=86400

r = redis.Redis(host='localhost', port=6379)

user = credentials['mysql']['user']
password = credentials['mysql']['password']
host = credentials['mysql']['host']
db = credentials['mysql']['db']

mydb = mysql.connector.connect(user = user, password = password, host = host, database = db)

mycursor = mydb.cursor()

for k, v in table_names.items():

	# Cache the date between present and data of a mongth ago 
    mycursor.execute(f'''SELECT t.Date, GROUP_CONCAT(DISTINCT CONCAT(Label, ',', Frequency) SEPARATOR ',') AS vals 
    	FROM {k} as t 
    	GROUP BY t.Date 
    	HAVING t.Date >= (CURDATE() - INTERVAL 1 MONTH);''')

    datum = mycursor.fetchall()

    for data in datum:
    	redis_key = v + "," + data[0].isoformat()
    	value_str = data[1]
    	r.set(redis_key, value_str, ex=EXPIRE_TIME)
    	print(f"get key={redis_key} from redis, value={r.get(redis_key)}")
        