#pragma once

#include "skiplist.h"
#include "utils.h"
#include "config.h"
#include <list>

class MemTable
 {
private:
    SkipList *skiplist;
    size_t size;

public:
    void put(uint64_t key, const std::string &s);

    bool check(uint64_t key);

    std::string get(uint64_t key);

    void get_data(std::vector<std::pair<uint64_t, std::string>> &data);

    void del(uint64_t key);

    void reset();

    void scan(uint64_t key1, uint64_t key2, std::list<std::pair<uint64_t, std::string>> &list);
};