#pragma once

struct KeyValue
{
    uint32_t key;
    uint32_t value;
};

struct HashTable {
    KeyValue *hashtable;
    uint32_t *size;
};

const uint32_t kHashTableCapacity = 128 * 1024 * 1024;

const uint32_t kNumKeyValues = kHashTableCapacity / 2;

const uint32_t kEmpty = 0xffffffff;

HashTable create_hashtable();

void insert_hashtable(HashTable& ht, const KeyValue* kvs, uint32_t num_kvs);

void lookup_hashtable(HashTable& ht, KeyValue* kvs, uint32_t num_kvs);

void delete_hashtable(HashTable& ht, const KeyValue* kvs, uint32_t num_kvs);

std::vector<KeyValue> iterate_hashtable(HashTable& ht);

void destroy_hashtable(HashTable& ht);
