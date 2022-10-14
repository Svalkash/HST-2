#include "algorithm"
#include "random"
#include "stdint.h"
#include "stdio.h"
#include "unordered_map"
#include "unordered_set"
#include "vector"
#include "chrono"
#include "linearprobing.h"

// Create random keys/values in the range [0, kEmpty)
// kEmpty is used to indicate an empty slot
std::vector<KeyValue> generate_random_keyvalues(std::mt19937& rnd, uint32_t numkvs)
{
    std::uniform_int_distribution<uint32_t> dis(0, kEmpty - 1);

    std::vector<KeyValue> kvs;
    kvs.reserve(numkvs);

    for (uint32_t i = 0; i < numkvs; i++)
    {
        uint32_t rand0 = dis(rnd);
        uint32_t rand1 = dis(rnd);
        kvs.push_back(KeyValue{rand0, rand1});
    }

    return kvs;
}

// return numshuffledkvs random items from kvs
std::vector<KeyValue> shuffle_keyvalues(std::mt19937& rnd, std::vector<KeyValue> kvs, uint32_t numshuffledkvs)
{
    std::shuffle(kvs.begin(), kvs.end(), rnd);

    std::vector<KeyValue> shuffled_kvs;
    shuffled_kvs.resize(numshuffledkvs);

    std::copy(kvs.begin(), kvs.begin() + numshuffledkvs, shuffled_kvs.begin());

    return shuffled_kvs;
}

using Time = std::chrono::time_point<std::chrono::high_resolution_clock>;

Time start_timer() 
{
    return std::chrono::high_resolution_clock::now();
}

double get_elapsed_time(Time start) 
{
    Time end = std::chrono::high_resolution_clock::now();

    std::chrono::duration<double> d = end - start;
    std::chrono::microseconds us = std::chrono::duration_cast<std::chrono::microseconds>(d);
    return us.count() / 1000.0f;
}

void test_unordered_map(std::vector<KeyValue> insert_kvs, std::vector<KeyValue> delete_kvs, uint32_t size) 
{
    Time timer = start_timer();

    printf("Timing std::unordered_map...\n");

    {
        std::unordered_map<uint32_t, uint32_t> kvs_map;
        for (auto& kv : insert_kvs) 
        {
            kvs_map[kv.key] = kv.value;
        }
        for (auto& kv : delete_kvs)
        {
            auto i = kvs_map.find(kv.key);
            if (i != kvs_map.end())
                kvs_map.erase(i);
        }
    }

    double milliseconds = get_elapsed_time(timer);
    double seconds = milliseconds / 1000.0f;
    printf("Total time for std::unordered_map: %f ms (%f million keys/second)\n", 
        milliseconds, size / seconds / 1000000.0f);
}

void test_correctness(std::vector<KeyValue>, std::vector<KeyValue>, std::vector<KeyValue>);

void default_test() {
    uint32_t default_cap = 1024 * 1024;
    uint32_t kv_size = default_cap * 32;
    // To recreate the same random numbers across runs of the program, set seed to a specific
    // number instead of a number from random_device
    std::random_device rd;
    uint32_t seed = rd();
    std::mt19937 rnd(seed);  // mersenne_twister_engine

    printf("Random number generator seed = %u\n", seed);

    while (true)
    {
        printf("Initializing keyvalue pairs with random numbers...\n");

        std::vector<KeyValue> insert_kvs = generate_random_keyvalues(rnd, kv_size);
        std::vector<KeyValue> delete_kvs = shuffle_keyvalues(rnd, insert_kvs, kv_size/2);

        // Begin test
        printf("Testing insertion/deletion of %d/%d elements into GPU hash table...\n",
            (uint32_t)insert_kvs.size(), (uint32_t)delete_kvs.size());

        Time timer = start_timer();

        HashTable pHashTable = create_hashtable(default_cap);

        // Insert items into the hash table
        const uint32_t num_insert_batches = 8*kv_size/default_cap;
        uint32_t num_inserts_per_batch = (uint32_t)insert_kvs.size() / num_insert_batches;
        for (uint32_t i = 0; i < num_insert_batches; i++)
        {
            insert_hashtable(pHashTable, insert_kvs.data() + i * num_inserts_per_batch, num_inserts_per_batch);
        }

        // Delete items from the hash table
        const uint32_t num_delete_batches = 8;
        uint32_t num_deletes_per_batch = (uint32_t)delete_kvs.size() / num_delete_batches;
        for (uint32_t i = 0; i < num_delete_batches; i++)
        {
            delete_hashtable(pHashTable, delete_kvs.data() + i * num_deletes_per_batch, num_deletes_per_batch);
        }

        // Get all the key-values from the hash table
        std::vector<KeyValue> kvs = iterate_hashtable(pHashTable);

        destroy_hashtable(pHashTable);

        // Summarize results
        double milliseconds = get_elapsed_time(timer);
        double seconds = milliseconds / 1000.0f;
        printf("Total time (including memory copies, readback, etc): %f ms (%f million keys/second)\n", milliseconds,
            default_cap/2 / seconds / 1000000.0f);

        test_unordered_map(insert_kvs, delete_kvs, default_cap/2);

        test_correctness(insert_kvs, delete_kvs, kvs);

        printf("Success\n");
    }
}

void csv_test() {
    uint32_t default_cap = 1024 * 1024;
    float min_thres = 0.3;
    float max_thres = 0.9;
    float step_thres = 0.05;
    uint32_t iter_per_thres = 10;

    uint32_t kv_size = default_cap * 16;
    // To recreate the same random numbers across runs of the program, set seed to a specific
    // number instead of a number from random_device
    std::random_device rd;
    uint32_t seed = rd();
    std::mt19937 rnd(seed);  // mersenne_twister_engine
    FILE *f = fopen("timing.csv", "w");

    printf("Random number generator seed = %u\n", seed);

    // Insert items into the hash table
    const uint32_t num_batches = 8*kv_size/default_cap;

    for (float thres = min_thres; thres < max_thres; thres += step_thres) {
        float round_sum = 0;
        float total_sum = 0;
        printf("Testing resize threshold = %f\n", thres);
        for (uint32_t iter = 0; iter <= iter_per_thres; ++iter) {
            fprintf(f, "%f, THRES, ", thres);
            std::vector<KeyValue> insert_kvs = generate_random_keyvalues(rnd, kv_size);
            std::vector<KeyValue> delete_kvs = shuffle_keyvalues(rnd, insert_kvs, kv_size/2);
            uint32_t num_inserts_per_batch = (uint32_t)insert_kvs.size() / num_batches;
            uint32_t num_deletes_per_batch = (uint32_t)delete_kvs.size() / num_batches;

            Time timer = start_timer();

            HashTable pHashTable = create_hashtable(default_cap, thres);

            for (uint32_t i = 0; i < num_batches; i++)
            {
                float round_time = insert_hashtable(pHashTable, insert_kvs.data() + i * num_inserts_per_batch, num_inserts_per_batch)
                    + delete_hashtable(pHashTable, delete_kvs.data() + i * num_deletes_per_batch, num_deletes_per_batch);
                if (iter == 0) {
                    fprintf(f, "%f, ", round_time);
                    round_sum += round_time;
                }
            }
            if (iter > 0) {
                // Get all the key-values from the hash table
                destroy_hashtable(pHashTable);
                double milliseconds = get_elapsed_time(timer);
                double seconds = milliseconds / 1000.0f;
                total_sum += milliseconds;
            }
        }
        float round_avg = round_sum / num_batches;
        float total_avg = total_sum / iter_per_thres;

        // Summarize results
        printf("AVERAGE round time: %f ms\n", round_avg);
        printf("AVERAGE total time: %f ms\n", total_avg);
        fprintf(f, "AVG, %lf, SUM, %lf\n", round_avg, total_avg);
    }
    fclose(f);
}

int main() 
{
    csv_test();
    return 0;
}
