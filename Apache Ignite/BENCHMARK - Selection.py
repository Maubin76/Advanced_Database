import random
import time
from pyignite import Client

# Setup
client = Client()
client.connect('127.0.0.1', 10800)

# Ensure the table exists
client.sql('''CREATE TABLE IF NOT EXISTS user_data (
                user_id VARCHAR PRIMARY KEY, 
                first_name VARCHAR, 
                last_name VARCHAR, 
                dob VARCHAR)''')

iterations = 40000

total_retrieval_time = 0

# Perform the retrieval 10000 times for a randomly selected ID
print("========== One element selection ==========")
for _ in range(iterations):
    # Select a random user_id between user:000001 and user:100000
    random_id = f"user:{random.randint(1, 100000):06d}" 

    # Measure retrieval time for the user data
    start_time = time.time()
    query = f"SELECT * FROM user_data WHERE user_id = ?"
    client.sql(query, query_args=[random_id])  # Execute the query
    end_time = time.time()

    total_retrieval_time += (end_time - start_time)

    # Print progress every 1000 iterations
    if _ % 1000 == 0:
        print(f"Selected {_:,} records...")

average_time_per_query = (total_retrieval_time / iterations) * 1000

print(f"---------- Apache Ignite ----------")
print(f"Average time for record retrieval: {average_time_per_query:.2f} ms")

client.close()
