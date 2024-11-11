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
print("========== 10K element selection ==========")
# ========== Benchmark Hashes ==========
print("---------- Hashes ----------")
# Number of repetitions for SCAN
iterations = 100
total_time = 0
count_nb = 10000  # Number of rows selected among the 2 million ones

for _ in range(iterations):
    start_time = time.time()
    client.scan(0, match="user:*", count=count_nb) # Equivalent to SELECT * FROM users WHERE username LIMIT 10000;
    end_time = time.time()
    total_time += (end_time - start_time)

average_time_ms_hashes = round((total_time / iterations) * 1000, 2)
print("Average time spent per 10k selections :", average_time_ms_hashes, "ms")

queries_per_second_hashes = int((iterations / total_time) * count_nb)
print("Selections per second :", queries_per_second_hashes)

# ========== Benchmark JSONs ==========
print("---------- JSONs ----------")
iterations = 10
total_time = 0
count_nb = 1000  # Number of rows to scan among the JSON keys

for _ in range(iterations):
    start_time = time.time()
    cursor = 0
    match_count = 0
    while match_count < count_nb:
        cursor, keys = client.scan(cursor=cursor, match="userJSON:*", count=count_nb)
        for key in keys:
            client.json().get(key, Path.root_path())  # Retrieves the entire JSON object
            match_count += 1
        if cursor == 0:
            break
    end_time = time.time()
    total_time += (end_time - start_time)

average_time_ms_jsons = round((total_time / iterations) * 1000, 2)
print("Average time spent per 10k selections :", average_time_ms_jsons, "ms")

queries_per_second_jsons = int((iterations / total_time) * count_nb)
print("Selections per second :", queries_per_second_jsons)

print()
# ========== Comparison of Specific ID Retrieval for Hash vs JSON ==========
print("========== One element selection ==========")
hash_retrieval_total = 0
json_retrieval_total = 0
iterations = 10000
# Perform the retrieval for a randomly selected ID
for _ in range(iterations):
    # Select a random ID between 1 and 2000000
    random_id = f"{random.randint(1, 2000000):06d}"

    start_time = time.time()
    client.hgetall(f"user:{random_id}")
    end_time = time.time()
    hash_retrieval_total += (end_time - start_time)

    start_time = time.time()
    client.json().get(f"userJSON:{random_id}", Path.root_path())
    end_time = time.time()
    json_retrieval_total += (end_time - start_time)

average_time_hash_retrieval = round((hash_retrieval_total / iterations) * 1000, 2)
average_time_json_retrieval = round((json_retrieval_total / iterations) * 1000, 2)

print("---------- Hashes ----------")
print("Average time for hash retrieval :", average_time_hash_retrieval, "ms")
print("---------- JSONs ----------")
print("Average time for JSON retrieval :", average_time_json_retrieval, "ms")
print()
