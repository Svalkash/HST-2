#pragma once

struct KeyValue
{
    uint32_t key;
    uint32_t value;
};

struct HashTable {
    KeyValue *hashtable;
    uint32_t *size; // occupied size, stored in GPU mem for better access
    uint32_t capacity; // max capacity (in key-value pairs)
    float resize_thres; // resize threshold
};

const uint32_t kEmpty = 0xffffffff;

HashTable create_hashtable(uint32_t capacity = 128 * 1024 * 1024, float resize_thres = 0.7); //set thres to >1.0 to disable

float insert_hashtable(HashTable& ht, const KeyValue* kvs, uint32_t num_kvs);

float lookup_hashtable(HashTable& ht, KeyValue* kvs, uint32_t num_kvs);

float delete_hashtable(HashTable& ht, const KeyValue* kvs, uint32_t num_kvs);

std::vector<KeyValue> iterate_hashtable(HashTable& ht);

void destroy_hashtable(HashTable& ht);

float resize_hashtable(HashTable& ht, uint32_t resize_k = 2);

float check_hashtable(HashTable &ht);