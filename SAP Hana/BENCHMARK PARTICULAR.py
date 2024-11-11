import time
from hdbcli import dbapi

iterations = 100
total_time = 0

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
    cursor = connection.cursor()

    for i in range(iterations):
        user_id = f"user:{i:06d}" 

        start_time = time.time()

        query = "SELECT * FROM users WHERE user_id = ?"

        # Execute the update query with provided values
        cursor.execute(query, user_id)
        

        end_time = time.time()
        total_time += (end_time - start_time)
        
    cursor.close()

finally:
    if connection.isconnected():
        connection.close()

average_time_ms_hashes = round((total_time / iterations) * 1000, 2)
print("Average time spent for retrieval:", average_time_ms_hashes, "ms")

