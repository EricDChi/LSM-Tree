#include <iostream>
#include "MurmurHash3.h"
#include "config.h"
#include <fstream>

class BloomFilter {
private:
    int bit_count;
    int hash_count;
    bool *bit;
    unsigned int *hash;

public:
    BloomFilter(int m, int k);
    ~BloomFilter();

    void add(uint64_t key);

    bool check(uint64_t key);

    void writeToFile(std::string path);

    void readFromFile(std::string path);
};

BloomFilter::BloomFilter(int m, int k) {
    this->bit_count = m;
    this->hash_count = k;
    bit = new bool[bit_count];
    hash = new unsigned int[hash_count];
}

BloomFilter::~BloomFilter() {
    
}

void BloomFilter::add(uint64_t key) {
    for (int i = 0; i <= (hash_count - 1) / 4; i++) {
        MurmurHash3_x64_128(&key, sizeof(key), i + 1, hash + 4 * i) ;
    }

    for(int i = 0; i < hash_count; i++) {
        bit[hash[i] % bit_count] = true;
    }
}

bool BloomFilter::check(uint64_t key) {
    for (int i = 0; i <= (hash_count - 1) / 4; i++) {
        MurmurHash3_x64_128(&key, sizeof(key), i + 1, hash + 4 * i) ;
    }
    for(int i = 0; i < hash_count; i++) {
        if(!bit[hash[i] % bit_count]) {
            return false;
        }
    }
    return true;
}

void BloomFilter::writeToFile(std::string path) {
    std::fstream sst_file(path, std::ios::out|std::ios::binary);

    if (sst_file) {
        sst_file.seekp(header_size, std::ios::beg);
        sst_file.write((char*)bit, bloomfilter_size);
        sst_file.close();
    }
}

void BloomFilter::readFromFile(std::string path) {
    std::fstream sst_file(path, std::ios::in|std::ios::binary);

    if (sst_file) {
        sst_file.seekg(header_size, std::ios::beg);
        sst_file.read((char*)bit, bloomfilter_size);
        sst_file.close();
    }
}