#pragma once

#include <iostream>
#include <vector>
#include <tuple>
#include <list>
#include <bitset>
#include "bloomfilter.h"
#include "config.h"
#include "utils.h"

class SStable{
private:
    BloomFilter *bloomfilter;
    uint64_t stamp, key_num, min_key, max_key;
    std::vector<std::tuple<uint64_t, uint64_t, uint32_t>> tuples;
    std::string path;

public:
    SStable(uint64_t SST_timeStamp, std::string path, std::string vlog, std::vector<std::pair<uint64_t, std::string>> &vector);
    SStable(uint64_t SST_timeStamp, std::string path, std::vector<std::tuple<uint64_t, uint64_t, uint32_t>> tuples);
    SStable(std::string fullPath);
    ~SStable();
    
    void put_vlog(std::string vlog, uint64_t key, uint32_t vlen, std::string &s, uint64_t &offset);
    
    std::string get(uint64_t key, std::string vlog);

    std::string get_without_bloomfilter(uint64_t key, std::string vlog);

    void scan(std::string vlog, uint64_t key1, uint64_t key2, std::list<std::pair<uint64_t, std::string>> &list);

    std::vector<std::tuple<uint64_t, uint64_t, uint32_t>> get_tuples() {return tuples;}

    std::tuple<uint64_t, uint64_t, uint32_t> get_tuple(uint64_t key);

    std::tuple<uint64_t, uint64_t, uint32_t> get_tuple_without_bloomfilter(uint64_t key);

    uint64_t get_stamp() {return stamp;}

    uint64_t get_offset(uint64_t key);

    uint64_t get_min_key() {return min_key;}

    uint64_t get_max_key() {return max_key;}

    std::string get_path() {return path;}
};