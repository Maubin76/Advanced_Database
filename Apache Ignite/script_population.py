import random
import datetime
from pyignite import Client
import time

# Function to generate a random date
def random_date():
    start_date = datetime.date(1950, 1, 1)
    end_date = datetime.date(2000, 12, 31)
    return start_date + datetime.timedelta(days=random.randint(0, (end_date - start_date).days))

# Setup Ignite client
client = Client()
client.connect('127.0.0.1', 10800)  # Adjust if needed

# Ensure the table exists
client.sql('''CREATE TABLE IF NOT EXISTS user_data (
                user_id VARCHAR PRIMARY KEY, 
                first_name VARCHAR, 
                last_name VARCHAR, 
                dob VARCHAR)''')

# Generate data and insert into Ignite
first_names = ['John', 'David', 'Mia', 'Sophia', 'Liam', 'Noah', 'Emma', 'Olivia', 'Ava', 'Isabella']
last_names = ['Doe', 'Bloom', 'Smith', 'Johnson', 'Williams', 'Brown', 'Jones', 'Garcia', 'Miller', 'Davis']

nb_rows = 50000
total_time = 0

try:
    for i in range(1, nb_rows + 1):
        user_id = f"user:{i:06}"
        first_name = random.choice(first_names)
        last_name = random.choice(last_names)
        dob = random_date().strftime("%d-%b-%Y").upper()

        start_time = time.time()
        # Execute the INSERT query
        client.sql('''INSERT INTO user_data (user_id, first_name, last_name, dob)
                      VALUES (?, ?, ?, ?)''', query_args=[user_id, first_name, last_name, dob])
        end_time = time.time()
        total_time = total_time + end_time - start_time
        if i % 1000 == 0:
            print(f"Inserted {i} rows...")

    print(f"Successfully inserted {nb_rows} rows in {total_time:.2f} seconds.")

finally:
    client.close()
