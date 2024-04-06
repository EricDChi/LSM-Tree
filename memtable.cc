#include "memtable.h"

MemTable::MemTable() {
    skiplist = new SkipList(0.5);
}

MemTable::~MemTable() {
    delete skiplist;
}

bool MemTable::check(uint64_t key) {
    Node* node = this->skiplist->find(key);
    if (node == nullptr && size + sizeof(uint64_t) * 2 + sizeof(uint32_t) > max_sst_size) {
        return false;  
    }
    return true;
}

std::string MemTable::get(uint64_t key) {
    std::string val = this->skiplist->get(key);
    return val;
}

void MemTable::get_data(std::vector<std::pair<uint64_t, std::string>> &data) {
    this->skiplist->get_data(data);
}

void MemTable::reset() {
    this->skiplist->clear();
}

void MemTable::put(uint64_t key, const std::string &s) {
    Node* node = this->skiplist->find(key);
    if (node == nullptr) {
        size += sizeof(uint64_t) * 2 + sizeof(uint32_t);
    }
    this->skiplist->put(key, s);
}

void MemTable::del(uint64_t key) {
    this->put(key, "~DELETE~");
}

void MemTable::scan(uint64_t key1, uint64_t key2, std::list<std::pair<uint64_t, std::string>> &list) {
    this->skiplist->scan(key1, key2, list);
}