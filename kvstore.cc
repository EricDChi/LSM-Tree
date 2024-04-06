#include "kvstore.h"
#include <string>
#include <fstream>
#include <vector>
#include "utils.h"

KVStore::KVStore(const std::string &dir) : KVStoreAPI(dir)
{
    this->dir = dir;
    memtable = new MemTable();
    max_SST_timeStamp = 0; 
	this->read_SSTFile(dir);
}

KVStore::~KVStore()
{
	std::vector<std::pair<uint64_t, std::string>> data;
    this->memtable->get_data(data);
    max_SST_timeStamp++;

    std::string new_filePath = dir + "/level-0" + std::to_string(max_SST_timeStamp) + ".sst";
	SStable *sstable = new SStable(max_SST_timeStamp, new_filePath, data);
	sst_vector.push_back({max_SST_timeStamp, sstable});

    this->memtable->reset();
	delete this->memtable;
	for (auto iter = sst_vector.begin(); iter != sst_vector.end(); iter++) {
		delete iter->second;
	}
	this->sst_vector.clear();
}

/**
 * Insert/Update the key-value pair.
 * No return values for simplicity.
 */
void KVStore::put(uint64_t key, const std::string &s)
{
    if (this->memtable->check(key) == true) {
        this->memtable->put(key, s);
        return;
    }
    
    std::vector<std::pair<uint64_t, std::string>> data;
    this->memtable->get_data(data);

    std::string new_filePath = dir + "/level-0" + std::to_string(max_SST_timeStamp) + ".sst";
	SStable *sstable = new SStable(max_SST_timeStamp, new_filePath, data);
	sst_vector.push_back({max_SST_timeStamp, sstable});

    this->memtable->reset();
	this->memtable->put(key, s);
	max_SST_timeStamp++;
}
/**
 * Returns the (string) value of the given key.
 * An empty string indicates not found.
 */
std::string KVStore::get(uint64_t key)
{
	std::string val = this->memtable->get(key);
	if (val == "~DELETE~") {
		return "";
	}
	else if (val != "") {
		return val;
	}
    
	for (auto iter = sst_vector.begin(); iter != sst_vector.end(); iter++) {
		val = iter->second->get(key);
		if (val != "") {
			return val;
		}
	}
	return "";
}
/**
 * Delete the given key-value pair if it exists.
 * Returns false iff the key is not found.
 */
bool KVStore::del(uint64_t key)
{
	if (this->get(key) != "") {
		this->memtable->del(key);
		return true;
	}
    return false;
}

/**
 * This resets the kvstore. All key-value pairs should be removed,
 * including memtable and all sstables files.
 */
void KVStore::reset()
{
	utils::rmfile(vlog_path);
	this->memtable->reset();
	for (auto iter = sst_vector.begin(); iter != sst_vector.end(); iter++) {
		delete iter->second;
	}
	this->sst_vector.clear();
}

/**
 * Return a list including all the key-value pair between key1 and key2.
 * keys in the list should be in an ascending order.
 * An empty string indicates not found.
 */
void KVStore::scan(uint64_t key1, uint64_t key2, std::list<std::pair<uint64_t, std::string>> &list)
{
	this->memtable->scan(key1, key2, list);
	for (auto iter = sst_vector.begin(); iter != sst_vector.end(); iter++) {
		auto sstable = iter->second;
	}
	sort(list.begin(), list.end());
}

/**
 * This reclaims space from vLog by moving valid value and discarding invalid value.
 * chunk_size is the size in byte you should AT LEAST recycle.
 */
void KVStore::gc(uint64_t chunk_size)
{
}

void KVStore::read_SSTFile(std::string dir) {
	if (!utils::dirExists(dir)) {
		utils::mkdir(dir);
	}
	std::string path = dir + "/level-0";
	if (!utils::dirExists(path)) {
		utils::mkdir(dir);
	}
	std::vector<std::string> ret;
	int sst_num = utils::scanDir(path, ret);
	max_SST_timeStamp += sst_num;
	for (int i = 0; i < sst_num; i++) {
		std::string fullPath = path + "/" + std::to_string(i) + ".sst";
		SStable *sstable = new SStable(fullPath);
		sst_vector.push_back({i, sstable});
	}
}