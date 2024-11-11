import time
from hdbcli import dbapi

iterations = 10
total_time = 0
count_nb = 10000

hana_address = 'e3bd540b-325a-4e89-8404-1e17a1a6cbb8.hna0.prod-eu10.hanacloud.ondemand.com'
hana_port = 443
hana_user = 'GE129777'
hana_password = 'Obxipal6tV1!'

# Connection to SAP Hana cloud
try:
    connection = dbapi.connect(
        address=hana_address,
        port=hana_port,
        user=hana_user,
        password=hana_password,
        encrypt="true",
        sslValidateCertificate="false"
    )

    # Query execution
    for _ in range(iterations):
        start_time = time.time()
        
        query = "SELECT * FROM users LIMIT 10000" 
        cursor = connection.cursor()  # Create a cursor to execute the query

        cursor.execute(query)         # Execute the query
        results = cursor.fetchall()   # Fetch the results if needed
        
        end_time = time.time()

        total_time += (end_time - start_time)

        cursor.close() 
    

finally:
    if connection.isconnected():
        connection.close()

average_time_ms_hashes = round((total_time / iterations) * 1000, 2)
print("Average time spent per 2M selections :", average_time_ms_hashes, "ms")
queries_per_second_hashes = int((iterations / total_time) * count_nb)
print("Selections per second :", queries_per_second_hashes)