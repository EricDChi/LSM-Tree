#pragma once

#include <iostream>
#include <vector>
#include <tuple>
#include <list>
#include "bloomfilter.h"
#include "config.h"

class SStable{
private:
    std::string path;
    BloomFilter *bloomfilter;
    uint64_t timeStamp, key_num, min_key, max_key;
    std::vector<std::tuple<uint64_t, off64_t, uint32_t>> tuples;

public:
    SStable(uint64_t SST_timeStamp, std::string path, std::vector<std::pair<uint64_t, std::string>> &vector);
    SStable(std::string fullPath);
    ~SStable();
    
    void put_vlog(uint64_t key, uint32_t vlen, std::string &s, off64_t &offset);
    
    std::string get(uint64_t key);

    void scan(uint64_t key1, uint64_t key2, std::list<std::pair<uint64_t, std::string>> &list);
};