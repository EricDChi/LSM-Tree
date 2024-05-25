#pragma once

#include <chrono>

#include "kvstore_api.h"
#include "skiplist.h"
#include "sstable.h"
#include "memtable.h"
#include "unordered_map"

class KVStore : public KVStoreAPI
{
	// You can add your implementation here
private:
	MemTable *memtable;
	uint64_t max_stamp;
	std::string dir;
	std::string vlog;
	std::vector<std::vector<SStable *>> sstables;
	uint64_t head, tail;

public:
	KVStore(const std::string &dir, const std::string &vlog);

	~KVStore();

	void put(uint64_t key, const std::string &s) override;

	std::string get(uint64_t key) override;

	bool del(uint64_t key) override;

	void reset() override;

	void scan(uint64_t key1, uint64_t key2, std::list<std::pair<uint64_t, std::string>> &list) override;

	void gc(uint64_t chunk_size) override;

	void read_sstables(std::string dir);

	void compaction(uint32_t level);

	uint64_t get_offset(uint64_t key);
};
