#include "stdio.h"
#include "stdint.h"
#include "vector"
#include "linearprobing.h"

// 32 bit Murmur3 hash
__device__ uint32_t hash(uint32_t k, uint32_t mod)
{
    k ^= k >> 16;
    k *= 0x85ebca6b;
    k ^= k >> 13;
    k *= 0xc2b2ae35;
    k ^= k >> 16;
    return k & (mod-1);
}

// Create a hash table. For linear probing, this is just an array of KeyValues
HashTable create_hashtable(uint32_t capacity) 
{
    // Allocate memory
    KeyValue* hashtable;
    uint32_t* size;
    cudaMalloc(&hashtable, sizeof(KeyValue) * capacity);
    cudaMalloc(&size, sizeof(uint32_t));

    // Initialize hash table to empty
    static_assert(kEmpty == 0xffffffff, "memset expected kEmpty=0xffffffff");
    cudaMemset(hashtable, 0xff, sizeof(KeyValue) * capacity);
    cudaMemset(size, 0x0, sizeof(uint32_t));

    uint32_t size1;
    cudaMemcpy(size, &size1, sizeof(uint32_t), cudaMemcpyDeviceToHost);
    printf("    space used: %d\n", size1);

    return { hashtable, size, capacity };
}

// Insert the key/values in kvs into the hashtable
__global__ void gpu_hashtable_insert(HashTable ht, const KeyValue* kvs, unsigned int numkvs)
{
    unsigned int threadid = blockIdx.x*blockDim.x + threadIdx.x;
    if (threadid < numkvs)
    {
        uint32_t key = kvs[threadid].key;
        uint32_t value = kvs[threadid].value;
        uint32_t slot = hash(key, ht.capacity);

        while (true)
        {
            uint32_t prev = atomicCAS(&ht.hashtable[slot].key, kEmpty, key);
            if (prev == kEmpty)
                atomicAdd(ht.size, 1); //new key space used
            if (prev == kEmpty || prev == key)
            {
                ht.hashtable[slot].value = value;
                return;
            }

            slot = (slot + 1) & (ht.capacity-1);
        }
    }
}
 
void insert_hashtable(HashTable& ht, const KeyValue* kvs, uint32_t num_kvs)
{
    // Copy the keyvalues to the GPU
    KeyValue* device_kvs;
    cudaMalloc(&device_kvs, sizeof(KeyValue) * num_kvs);
    cudaMemcpy(device_kvs, kvs, sizeof(KeyValue) * num_kvs, cudaMemcpyHostToDevice);

    // Have CUDA calculate the thread block size
    int mingridsize;
    int threadblocksize;
    cudaOccupancyMaxPotentialBlockSize(&mingridsize, &threadblocksize, gpu_hashtable_insert, 0, 0);

    // Create events for GPU timing
    cudaEvent_t start, stop;
    cudaEventCreate(&start);
    cudaEventCreate(&stop);

    cudaEventRecord(start);

    // Insert all the keys into the hash table
    int gridsize = ((uint32_t)num_kvs + threadblocksize - 1) / threadblocksize;
    gpu_hashtable_insert<<<gridsize, threadblocksize>>>(ht, device_kvs, (uint32_t)num_kvs);

    cudaEventRecord(stop);

    cudaEventSynchronize(stop);

    float milliseconds = 0;
    cudaEventElapsedTime(&milliseconds, start, stop);
    float seconds = milliseconds / 1000.0f;
    printf("    GPU inserted %d items in %f ms (%f million keys/second)\n", 
        num_kvs, milliseconds, num_kvs / (double)seconds / 1000000.0f);

    uint32_t size;
    cudaMemcpy(&size, ht.size, sizeof(uint32_t), cudaMemcpyDeviceToHost);
    printf("    space used: %d\n", size);

    cudaFree(device_kvs);
}

// Lookup keys in the hashtable, and return the values
__global__ void gpu_hashtable_lookup(HashTable ht, KeyValue* kvs, unsigned int numkvs)
{
    unsigned int threadid = blockIdx.x * blockDim.x + threadIdx.x;
    if (threadid < ht.capacity)
    {
        uint32_t key = kvs[threadid].key;
        uint32_t slot = hash(key, ht.capacity);

        while (true)
        {
            if (ht.hashtable[slot].key == key)
            {
                kvs[threadid].value = ht.hashtable[slot].value;
                return;
            }
            if (ht.hashtable[slot].key == kEmpty)
            {
                kvs[threadid].value = kEmpty;
                return;
            }
            slot = (slot + 1) & (ht.capacity - 1);
        }
    }
}

void lookup_hashtable(HashTable& ht, KeyValue* kvs, uint32_t num_kvs)
{
    // Copy the keyvalues to the GPU
    KeyValue* device_kvs;
    cudaMalloc(&device_kvs, sizeof(KeyValue) * num_kvs);
    cudaMemcpy(device_kvs, kvs, sizeof(KeyValue) * num_kvs, cudaMemcpyHostToDevice);

    // Have CUDA calculate the thread block size
    int mingridsize;
    int threadblocksize;
    cudaOccupancyMaxPotentialBlockSize(&mingridsize, &threadblocksize, gpu_hashtable_insert, 0, 0);

    // Create events for GPU timing
    cudaEvent_t start, stop;
    cudaEventCreate(&start);
    cudaEventCreate(&stop);

    cudaEventRecord(start);

    // Insert all the keys into the hash table
    int gridsize = ((uint32_t)num_kvs + threadblocksize - 1) / threadblocksize;
    gpu_hashtable_insert << <gridsize, threadblocksize >> > (ht, device_kvs, (uint32_t)num_kvs);

    cudaEventRecord(stop);

    cudaEventSynchronize(stop);

    float milliseconds = 0;
    cudaEventElapsedTime(&milliseconds, start, stop);
    float seconds = milliseconds / 1000.0f;
    printf("    GPU lookup %d items in %f ms (%f million keys/second)\n",
        num_kvs, milliseconds, num_kvs / (double)seconds / 1000000.0f);

    cudaFree(device_kvs);
}

// Delete each key in kvs from the hash table, if the key exists
// A deleted key is left in the hash table, but its value is set to kEmpty
// Deleted keys are not reused; once a key is assigned a slot, it never moves
__global__ void gpu_hashtable_delete(HashTable ht, const KeyValue* kvs, unsigned int numkvs)
{
    unsigned int threadid = blockIdx.x * blockDim.x + threadIdx.x;
    if (threadid < ht.capacity)
    {
        uint32_t key = kvs[threadid].key;
        uint32_t slot = hash(key, ht.capacity);

        while (true)
        {
            if (ht.hashtable[slot].key == key)
            {
                ht.hashtable[slot].value = kEmpty;
                return;
            }
            if (ht.hashtable[slot].key == kEmpty)
            {
                return;
            }
            slot = (slot + 1) & (ht.capacity - 1);
        }
    }
}

void delete_hashtable(HashTable& ht, const KeyValue* kvs, uint32_t num_kvs)
{
    // Copy the keyvalues to the GPU
    KeyValue* device_kvs;
    cudaMalloc(&device_kvs, sizeof(KeyValue) * num_kvs);
    cudaMemcpy(device_kvs, kvs, sizeof(KeyValue) * num_kvs, cudaMemcpyHostToDevice);

    // Have CUDA calculate the thread block size
    int mingridsize;
    int threadblocksize;
    cudaOccupancyMaxPotentialBlockSize(&mingridsize, &threadblocksize, gpu_hashtable_insert, 0, 0);

    // Create events for GPU timing
    cudaEvent_t start, stop;
    cudaEventCreate(&start);
    cudaEventCreate(&stop);

    cudaEventRecord(start);

    // Insert all the keys into the hash table
    int gridsize = ((uint32_t)num_kvs + threadblocksize - 1) / threadblocksize;
    gpu_hashtable_delete<< <gridsize, threadblocksize >> > (ht, device_kvs, (uint32_t)num_kvs);

    cudaEventRecord(stop);

    cudaEventSynchronize(stop);

    float milliseconds = 0;
    cudaEventElapsedTime(&milliseconds, start, stop);
    float seconds = milliseconds / 1000.0f;
    printf("    GPU delete %d items in %f ms (%f million keys/second)\n",
        num_kvs, milliseconds, num_kvs / (double)seconds / 1000000.0f);

    cudaFree(device_kvs);
}

// Iterate over every item in the hashtable; return non-empty key/values
__global__ void gpu_iterate_hashtable(HashTable ht, KeyValue* kvs, uint32_t* kvs_size)
{
    unsigned int threadid = blockIdx.x * blockDim.x + threadIdx.x;
    if (threadid < ht.capacity) 
    {
        if (ht.hashtable[threadid].key != kEmpty) 
        {
            uint32_t value = ht.hashtable[threadid].value;
            if (value != kEmpty)
            {
                uint32_t size = atomicAdd(kvs_size, 1);
                kvs[size] = ht.hashtable[threadid];
            }
        }
    }
}

std::vector<KeyValue> iterate_hashtable(HashTable &ht)
{
    uint32_t* device_num_kvs;
    cudaMalloc(&device_num_kvs, sizeof(uint32_t));
    cudaMemset(device_num_kvs, 0, sizeof(uint32_t));

    KeyValue* device_kvs;
    cudaMalloc(&device_kvs, sizeof(KeyValue) * ht.capacity/2);

    int mingridsize;
    int threadblocksize;
    cudaOccupancyMaxPotentialBlockSize(&mingridsize, &threadblocksize, gpu_iterate_hashtable, 0, 0);

    int gridsize = (ht.capacity + threadblocksize - 1) / threadblocksize;
    gpu_iterate_hashtable<<<gridsize, threadblocksize>>>(ht, device_kvs, device_num_kvs);

    uint32_t num_kvs;
    cudaMemcpy(&num_kvs, device_num_kvs, sizeof(uint32_t), cudaMemcpyDeviceToHost);

    std::vector<KeyValue> kvs;
    kvs.resize(num_kvs);

    cudaMemcpy(kvs.data(), device_kvs, sizeof(KeyValue) * num_kvs, cudaMemcpyDeviceToHost);

    cudaFree(device_kvs);
    cudaFree(device_num_kvs);

    return kvs;
}

// Free the memory of the hashtable
void destroy_hashtable(HashTable &ht)
{
    cudaFree(ht.hashtable);
    cudaFree(ht.size);
}

// Move the original kv into the new one
__global__ void gpu_hashtable_move(HashTable ht, HashTable new_ht)
{
    unsigned int threadid = blockIdx.x*blockDim.x + threadIdx.x;
    if (threadid < ht.capacity)
    {
        uint32_t key = ht.hashtable[threadid].key;
        uint32_t value = ht.hashtable[threadid].value;
        uint32_t slot = hash(key, new_ht.capacity);
        if (key == kEmpty || value == kEmpty) return; //skip empty and deleted

        //copied from the basic insertion
        while (true)
        {
            uint32_t prev = atomicCAS(&new_ht.hashtable[slot].key, kEmpty, key);
            if (prev == kEmpty)
                atomicAdd(new_ht.size, 1); //new key space used
            if (prev == kEmpty || prev == key)
            {
                new_ht.hashtable[slot].value = value;
                return;
            }

            slot = (slot + 1) & (new_ht.capacity-1);
        }
    }
}
 
void resize_hashtable(HashTable& ht, uint32_t resize_k)
{
    HashTable new_ht = { nullptr, nullptr, ht.capacity * resize_k }

    // Allocate mem for the new table
    cudaMalloc(&new_ht.hashtable, sizeof(KeyValue) * new_ht.capacity);
    cudaMalloc(&new_ht.size, sizeof(uint32_t));

    // Initialize new table to empty
    static_assert(kEmpty == 0xffffffff, "memset expected kEmpty=0xffffffff");
    cudaMemset(new_ht.hashtable, 0xff, sizeof(KeyValue) * new_ht.capacity);
    cudaMemset(new_ht.size, 0x0, sizeof(uint32_t));

    // Now copy NON-empty keys and their values
    // Have CUDA calculate the thread block size
    int mingridsize;
    int threadblocksize;
    cudaOccupancyMaxPotentialBlockSize(&mingridsize, &threadblocksize, gpu_hashtable_resize, 0, 0);

    // Create events for GPU timing
    cudaEvent_t start, stop;
    cudaEventCreate(&start);
    cudaEventCreate(&stop);

    cudaEventRecord(start);

    // Insert all the keys into the hash table
    int gridsize = (ht.capacity + threadblocksize - 1) / threadblocksize;
    gpu_hashtable_move<<<gridsize, threadblocksize>>>(ht, new_ht);

    cudaEventRecord(stop);

    cudaEventSynchronize(stop);

    float milliseconds = 0;
    cudaEventElapsedTime(&milliseconds, start, stop);
    printf("    GPU moved %d items in %f ms\n", 
        *new_ht.size, milliseconds);

    uint32_t size;
    cudaMemcpy(&size, new_ht.size, sizeof(uint32_t), cudaMemcpyDeviceToHost);
    printf("    space used: %d\n", size);

    //nuke the old table and reassign it to the new one
    cudaFree(ht.hashtable);
    cudaFree(ht.size);
    ht = new_ht;
}