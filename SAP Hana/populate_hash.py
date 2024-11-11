import random
import datetime
import csv

def random_date():
    start_date = datetime.date(1950, 1, 1)
    end_date = datetime.date(2000, 12, 31)
    time_between_dates = end_date - start_date
    days_between_dates = time_between_dates.days
    random_number_of_days = random.randrange(days_between_dates)
    return start_date + datetime.timedelta(days=random_number_of_days)

first_names = ['John', 'David', 'Mia', 'Sophia', 'Liam', 'Noah', 'Emma', 'Olivia', 'Ava', 'Isabella']
last_names = ['Doe', 'Bloom', 'Smith', 'Johnson', 'Williams', 'Brown', 'Jones', 'Garcia', 'Miller', 'Davis']

# Open csv file in writing mode
with open('populate_users.csv', mode='w', newline='', encoding='utf-8') as file:
    writer = csv.writer(file)
    
    # write the header
    writer.writerow(['USER_ID', 'FIRST_NAME', 'LAST_NAME', 'DOB'])
    
    # Generate data
    for i in range(1, 2000001):
        user_id = f"user:{i:06}"
        first_name = random.choice(first_names)
        last_name = random.choice(last_names)
        dob = random_date().strftime("%Y-%m-%d")
        
        # Write data in the csv file
        writer.writerow([user_id, first_name, last_name, dob])

print("CSV file for 2,000,000 users generated and saved to 'populate_users.csv'")
