#pragma once

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
    BloomFilter(int m, int k, bool *bits);

    BloomFilter(int m, int k);

    ~BloomFilter();

    void add(uint64_t key);

    bool check(uint64_t key);

    void writeToFile(std::fstream &sst_file);
};
