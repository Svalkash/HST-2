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
};

const uint32_t kEmpty = 0xffffffff;

HashTable create_hashtable(uint32_t capacity = 128 * 1024 * 1024);

void insert_hashtable(HashTable& ht, const KeyValue* kvs, uint32_t num_kvs);

void lookup_hashtable(HashTable& ht, KeyValue* kvs, uint32_t num_kvs);

void delete_hashtable(HashTable& ht, const KeyValue* kvs, uint32_t num_kvs);

std::vector<KeyValue> iterate_hashtable(HashTable& ht);

void destroy_hashtable(HashTable& ht);

void resize_hashtable(HashTable& ht, uint32_t resize_k = 2);

void check_hashtable(HashTable &ht, float resize_thres = 0.7); //set thres to >1.0 to disable