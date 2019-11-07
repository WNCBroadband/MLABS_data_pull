from google.cloud import bigquery
import os
from datetime import datetime
import pymysql

print("Bigquery and PyMySQL imported")
# connecting to BigQuery
print("Exporting BigQuery Credentials")
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "C:\Development\DEV_Code\WestNGN_MLABs_Data\westngn-256318-b53ec54b351a.json"
print("Verifying Credentials")

# now to connect to the mysql database
print("Connecting to MySQL Database")

db = pymysql.connect(
  host="138.68.228.126",
  user="drawertl_westngn",
  passwd="dbpass_adh4enal",
  database="drawertl_westngn"
)

print("Connected to database: drawertl_westngn, Creating cursor object.")

cursor = db.cursor()

print("Cursor created.")


# back to bigquery
client = bigquery.Client()
print("Running Query")

query_job = client.query("""
    SELECT
      8 * (web100_log_entry.snap.HCThruOctetsAcked /
        (web100_log_entry.snap.SndLimTimeRwin +
        web100_log_entry.snap.SndLimTimeCwnd +
        web100_log_entry.snap.SndLimTimeSnd)) AS download_Mbps,
      8 * (web100_log_entry.snap.HCThruOctetsReceived /
          web100_log_entry.snap.Duration) AS upload_Mbps,

        connection_spec.client_geolocation.latitude AS client_lat, 
        connection_spec.client_geolocation.longitude AS client_lon,
        connection_spec.client_ip AS client_ip,
        log_time,
        FORMAT_DATETIME("%F %X", DATETIME(log_time, "UTC")) AS client_test_time,
        test_id 
    FROM
      `measurement-lab.ndt.web100`
    WHERE
      partition_date BETWEEN '2019-10-01' AND \'""" + str(datetime.today().strftime('%Y-%m-%d')) + """\'
      AND connection_spec.data_direction = 1
      AND web100_log_entry.snap.HCThruOctetsAcked >= 8192
      AND (web100_log_entry.snap.SndLimTimeRwin +
        web100_log_entry.snap.SndLimTimeCwnd +
        web100_log_entry.snap.SndLimTimeSnd) >= 9000000
      AND (web100_log_entry.snap.SndLimTimeRwin +
        web100_log_entry.snap.SndLimTimeCwnd +
        web100_log_entry.snap.SndLimTimeSnd) < 600000000
      AND web100_log_entry.snap.CongSignals > 0
      AND (web100_log_entry.snap.State = 1 OR
        (web100_log_entry.snap.State >= 5 AND
        web100_log_entry.snap.State <= 11))
      AND (connection_spec.client_geolocation.latitude BETWEEN 35.50 AND 35.60)
      AND (connection_spec.client_geolocation.longitude BETWEEN -82.80 AND -82.50)
""")

print("Query Made, Fetching Results")
    
results = query_job.result()  # Waits for job to complete.

print("Query Results Fetched")

#now
i = 0
for row in results:
    i = i + 1

#print("|IP({}) |:| Download({}Mbps)|:| Upload({}Mbps) |:| Coords({},{}) |:| Time({}) |:| Test_ID({})|\n".format(row.client_ip, row.download_Mbps, row.upload_Mbps, row.client_lat, row.client_lon, row.client_test_time, row.test_id))

    values = ("\'{}\', \'{}\', \'{}\', {}, {}, {}, {}".format(row.client_ip, row.test_id, row.client_test_time, row.client_lat, row.client_lon, row.download_Mbps, row.upload_Mbps,))
    print("Inserting query result " + str(i) + " into the MLABS_speed_data table")
#    print("values = " + values + "\n")

# Prepare SQL query to INSERT a record into the database.
    sql = """INSERT INTO MLABS_speed_data(
        ip_address,
         mlabs_test_id,
         date_taken,
         latitude,
         longitude,
         download_speed,
         upload_speed
         )
         VALUES (""" + values + """)"""
#    print(sql)
    
    
    try:
       # Execute the SQL command
       cursor.execute(sql)
       # Commit your changes in the database
       db.commit()
       print("\nINSERT success!")
    except:
       # Rollback in case there is any error
       db.rollback()
       print("\n*** ERROR ***\n INSERT FAILED \n*** ERROR ***\n")

print("accessing table MLABS_speed_data...")

cursor.execute("SELECT * FROM MLABS_speed_data")

table_result = cursor.fetchall()

print("Table MLABS_speed_data fetched. Now printing the table...")

for x in table_result:
  print(x)

db.close()
