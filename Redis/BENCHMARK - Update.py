import time
import redis
import random
from redis.commands.json.path import Path

# Connection with authentication
client = redis.Redis(
    host='localhost',
    port=6379,
    password='mypassword'
)

print()
print("========== 50K element update ==========")

# ========== Update Hashes ==========
print("---------- Hashes ----------")
iterations = 50000
total_time = 0

# Start updating 50,000 hashes
for i in range(1, iterations + 1):
    user_id = f"user:{i:06d}" 
    new_last_name = f"Updated_{i}"  # New value to update

    start_time = time.time()
    client.hset(user_id, "last_name", new_last_name)
    end_time = time.time()
    total_time += (end_time - start_time)

average_time_ms_hashes = round((total_time / iterations) * 1000, 2)
print("Average time spent per update:", average_time_ms_hashes, "ms")

updates_per_second_hashes = int(iterations / total_time)
print("Updates per second:", updates_per_second_hashes)


# ========== Update JSONs ==========
print("---------- JSONs ----------")
iterations = 50000  # Number of elements to update
total_time = 0

# Start updating 50,000 JSON objects
for i in range(1, iterations + 1):
    user_id = f"userJSON:{i:06d}" 
    new_last_name = f"Updated_{i}"  # New value to update, just for testing

    start_time = time.time()
    client.json().set(user_id, Path("$.last_name"), new_last_name)
    end_time = time.time()
    total_time += (end_time - start_time)

average_time_ms_jsons = round((total_time / iterations) * 1000, 2)
print("Average time spent per update:", average_time_ms_jsons, "ms")

updates_per_second_jsons = int(iterations / total_time)
print("Updates per second:", updates_per_second_jsons)