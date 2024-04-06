#pragma once

#include <algorithm>
#include "memtable.h"
#include "kvstore_api.h"
#include "sstable.h"

class KVStore : public KVStoreAPI
{
	// You can add your implementation here
private:
	MemTable *memtable;
	int max_SST_timeStamp;
	std::string dir;
	std::vector<std::pair<uint64_t, SStable*>> sst_vector;

public:
	KVStore(const std::string &dir);

	~KVStore();

	void put(uint64_t key, const std::string &s) override;

	std::string get(uint64_t key) override;

	bool del(uint64_t key) override;

	void reset() override;

	void scan(uint64_t key1, uint64_t key2, std::list<std::pair<uint64_t, std::string>> &list) override;

	void read_SSTFile(std::string path);

	void gc(uint64_t chunk_size) override;
};
