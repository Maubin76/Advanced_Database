import random
import time
from pyignite import Client

# Setup
client = Client()
client.connect('127.0.0.1', 10800)

# Lists of first and last names for updating records
first_names = ['John', 'David', 'Mia', 'Sophia', 'Liam', 'Noah', 'Emma', 'Olivia', 'Ava', 'Isabella']
last_names = ['Doe', 'Bloom', 'Smith', 'Johnson', 'Williams', 'Brown', 'Jones', 'Garcia', 'Miller', 'Davis']

num_updates = 50000
total_update_time = 0

try:
    for i in range(1, num_updates + 1):
        # Select a random user_id between user:000001 and user:100000
        user_id = f"user:{random.randint(1, 100000):06d}"
        
        new_first_name = random.choice(first_names)
        new_last_name = random.choice(last_names)
        
        start_time = time.time()
        
        # Execute the UPDATE query
        client.sql('''UPDATE user_data SET first_name = ?, last_name = ? WHERE user_id = ?''',
                   query_args=[new_first_name, new_last_name, user_id])
        
        end_time = time.time()
        
        total_update_time += (end_time - start_time)
        
        # Print progress every 5000 updates
        if i % 5000 == 0:
            print(f"Updated {i} records...")

    average_update_time = (total_update_time / num_updates) * 1000
    print(f"Successfully updated {num_updates} records.")
    print(f"Total update time: {total_update_time:.2f} seconds.")
    print(f"Average update time per record: {average_update_time:.2f} ms")

finally:
    client.close()
