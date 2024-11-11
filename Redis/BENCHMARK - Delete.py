import redis
import random
import time

# Connection
client = redis.Redis(
    host='localhost',
    port=6379,
    password='mypassword'
)

num_deletes = 100000
total_delete_time = 0

try:
    for i in range(1, num_deletes + 1):
        # Select a random user ID in the range user:000001 to user:2000000
        user_id = f"user:{random.randint(1, 2000000):06d}"
        
        start_time = time.time()
        
        # Execute the DELETE operation for the hash key
        client.delete(user_id)
        
        end_time = time.time()
        
        total_delete_time += (end_time - start_time)
        
        # Print progress every 10,000 deletions
        if i % 10000 == 0:
            print(f"Deleted {i} hashes...")

    average_delete_time = (total_delete_time / num_deletes) * 1000
    print(f"Successfully deleted {num_deletes} hashes.")
    print(f"Total delete time: {total_delete_time:.2f} seconds.")
    print(f"Average delete time per hash: {average_delete_time:.2f} ms")

except Exception as e:
    print("An error occurred:", e)

finally:
    client.close()
