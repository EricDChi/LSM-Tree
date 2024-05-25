#include "kvstore.h"
#include <string>

using namespace utils;

KVStore::KVStore(const std::string &dir, const std::string &vlog) : KVStoreAPI(dir, vlog)
{
	this->dir = dir;
	this->vlog = vlog;
	this->max_stamp = 0;
	this->memtable = new MemTable();
	if (dirExists(dir)) {
		std::ifstream file(vlog, std::ios::binary|std::ios::ate);
    	if (file) {
			uint8_t byte;
			uint8_t magic;
			uint16_t checksum;
			uint64_t key;
			uint32_t vlen;
			std::string str;
			head = file.tellg();

			for (tail = seek_data_block(vlog); tail < head; tail++) {
		 		file.seekg(tail);
         		file.read(reinterpret_cast<char*>(&byte), sizeof(byte));

         		if (byte != 0xFF) {  // Check if this is the Magic byte
		 			continue;
		 		}
            
		 		// Attempt to read the rest of the entry from this position
         		file.seekg(tail);
         		file.read(reinterpret_cast<char*>(&magic), sizeof(magic));
         		file.read(reinterpret_cast<char*>(&checksum), sizeof(checksum));
         		file.read(reinterpret_cast<char*>(&key), sizeof(key));
         		file.read(reinterpret_cast<char*>(&vlen), sizeof(vlen));
				std::vector<char> buffer(vlen);
		 		file.read(buffer.data(), vlen);

         		// Check if the entry data can be fully read
         		if (vlen > 0 && head - file.tellg() >= vlen) {
             		// Validate checksum
					std::string val(buffer.begin(), buffer.end());
		 			str = std::to_string(key) + val + std::to_string(vlen);
             		uint16_t calculated_checksum = utils::crc16(std::vector<unsigned char>(str.begin(), str.end()));
             		if (calculated_checksum == checksum) {
						break;
		 			}			
		 		}
			}
			file.close();
    	}
		this->read_sstables(dir);
	}
	if (sstables.empty()) {
		sstables.push_back(std::vector<SStable*>());
	}
}

KVStore::~KVStore()
{
	std::vector<std::pair<uint64_t, std::string>> data;
	this->memtable->get_data(data);

	if (!dirExists(dir + "/level-0")) {
		mkdir(dir + "/level-0");
	}	

	if (!data.empty()) {
		std::string new_filePath = dir + "/level-0/" + std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()) + ".sst";
		SStable *sstable = new SStable(max_stamp, new_filePath, vlog, data);
		sstables[0].push_back(sstable);
		compaction(0);
	}

	this->memtable->reset();
	delete this->memtable;
	for (size_t i = 0; i < sstables.size(); i++) {
		for (size_t j = 0; j < sstables.at(i).size(); j++) {
			SStable *tmp = sstables[i].at(j);
			sstables[i].erase(sstables[i].begin() + j);
			delete tmp;
	 	}
		this->sstables[i].clear();
	}
	this->sstables.clear();
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

	if (!dirExists(dir + "/level-0")) {
		mkdir(dir + "/level-0");
	}
    std::string new_filePath = dir + "/level-0/" + std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()) + ".sst";
	SStable *sstable = new SStable(max_stamp, new_filePath, vlog, data);
	sstables[0].push_back(sstable);
	compaction(0);

	max_stamp++;
    this->memtable->reset();
	this->memtable->put(key, s);
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
    
	for (size_t i = 0; i < sstables.size(); i++) {
		for (size_t j = sstables.at(i).size(); j > 0; j--) {
			val = sstables.at(i).at(j - 1)->get(key, vlog);
			if (val != "") {
				if (val == "~DELETE~") {
					return "";
				}
				return val;
			}
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
		this->put(key, "~DELETE~");
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
	max_stamp = 1;
	head = 0, tail = 0;
	std::vector<std::string> vec_level, vec_sstable;
	if (dirExists(dir)) {
		rmfile(vlog);
		scanDir(dir, vec_level);
		for (size_t i = 0; i < vec_level.size(); i++) {
			scanDir(dir + "/" + vec_level.at(i), vec_sstable);
			for (size_t j = 0; j < vec_sstable.size(); j++) {
				rmfile((dir + "/" + vec_level.at(i) + "/" + vec_sstable.at(j)).c_str());
			}
			rmdir((dir + "/" + vec_level.at(i)).c_str());
		}
		rmdir(dir);
	}
	this->memtable->reset();
	for (size_t i = 0; i < sstables.size(); i++) {
		for (size_t j = 0; j < sstables.at(i).size(); j++) {
			delete sstables.at(i).at(j);
		}
		sstables.at(i).clear();
	}
	sstables.clear();
	sstables.push_back(std::vector<SStable*>());
}

/**
 * Return a list including all the key-value pair between key1 and key2.
 * keys in the list should be in an ascending order.
 * An empty string indicates not found.
 */
void KVStore::scan(uint64_t key1, uint64_t key2, std::list<std::pair<uint64_t, std::string>> &list)
{
	this->memtable->scan(key1, key2, list);
	for (size_t i = 0; i < sstables.size(); i++) {
		std::sort(sstables.at(i).begin(), sstables.at(i).end(), [](SStable* a, SStable* b) { // Remove const qualifier from lambda function
        	return a->get_stamp() < b->get_stamp();
    	});
		for (size_t j = 0; j < sstables.at(i).size(); j++) {
			sstables.at(i).at(j)->scan(vlog, key1, key2, list);
		}
	}
	list.sort([](const std::pair<uint64_t, std::string>& a, const std::pair<uint64_t, std::string>& b) {
        return a.first < b.first;
    });
}

/**
 * This reclaims space from vLog by moving valid value and discarding invalid value.
 * chunk_size is the size in byte you should AT LEAST recycle.
 */
void KVStore::gc(uint64_t chunk_size)
{
	std::ifstream file(vlog, std::ios::binary|std::ios::ate);
    if (!file.is_open()) {
        throw std::runtime_error("Failed to open the file: " + vlog);
		return;
    }

	uint64_t startPosition = tail;
	uint8_t byte;
	uint8_t magic;
	uint16_t checksum;
	uint64_t key;
	uint32_t vlen;
	std::string str;
	head = file.tellg();

	uint64_t i = startPosition;
    // Start from the end of the file and move backwards to find the last Magic byte
	for (; i < head; i++) {
        file.seekg(i);
        file.read(reinterpret_cast<char*>(&byte), sizeof(byte));

        if (byte != 0xff) {  // Check if this is the Magic byte
	 		continue;
	 	}
            
	 	// Attempt to read the rest of the entry from this position
        file.seekg(i);
        file.read(reinterpret_cast<char*>(&magic), sizeof(magic));
        file.read(reinterpret_cast<char*>(&checksum), sizeof(checksum));
        file.read(reinterpret_cast<char*>(&key), sizeof(key));
        file.read(reinterpret_cast<char*>(&vlen), sizeof(vlen));
	 	std::vector<char> buffer(vlen);
	 	file.read(buffer.data(), vlen);

        // Check if the entry data can be fully read
        if (vlen > 0 && head - file.tellg() >= vlen) {
             // Validate checksum
	 		std::string val(buffer.begin(), buffer.end());
	 		str = std::to_string(key) + val + std::to_string(vlen);
            uint16_t calculated_checksum = crc16(std::vector<unsigned char>(str.begin(), str.end()));
	 		if (calculated_checksum != checksum) {
	 			continue;
	 		}

			if (this->get_offset(key) != 0 && this->get_offset(key) == i + vlog_entry_header_size) {
	 			this->put(key, val);
	 		}
	 		i += (vlog_entry_header_size + vlen - 1);
	 		if (i - startPosition >= (uint64_t)chunk_size) {
	 			tail = i + 1;
	 			break;
	 		}
        }
	}

	std::vector<std::pair<uint64_t, std::string>> data;
	this->memtable->get_data(data);
	if (data.empty()) {
	 	de_alloc_file(vlog, startPosition, tail - startPosition);
	 	return;
	}	

	if (!dirExists(dir + "/level-0")) {
		mkdir(dir + "/level-0");
	}
    std::string new_filePath = dir + "/level-0/" + std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()) + ".sst";
	SStable *sstable = new SStable(max_stamp, new_filePath, vlog, data);
	sstables[0].push_back(sstable);
	compaction(0);

	max_stamp++;
    this->memtable->reset();

	de_alloc_file(vlog, startPosition, tail - startPosition);
}

void KVStore::read_sstables(std::string dir)
{
	std::vector<std::string> vec_level, vec_sstable;
    scanDir(dir, vec_level);
	sstables.resize(vec_level.size());
    for (size_t i = 0; i < vec_level.size(); i++) {
		if (vec_level.at(i) == "vlog") {
			continue;
		}
        
		int level = std::stoi(vec_level.at(i).substr(6));
        scanDir(dir + "/" + vec_level.at(i), vec_sstable);

        for (size_t j = 0; j < vec_sstable.size(); j++) {
            std::string path = dir + "/" + vec_level.at(i) + "/" + vec_sstable.at(j);
            SStable* sst = new SStable(path);
            sstables[level].push_back(sst);

            if (sst->get_stamp() > max_stamp) {
                max_stamp = sst->get_stamp();
            }
        }
        vec_sstable.clear();
    }
}

uint64_t KVStore::get_offset(uint64_t key)
{
	std::string val = this->memtable->get(key);
	if (val == "~DELETE~") {
		return 0;
	}
	else if (val != "") {
		return 0;
	}
    
	uint64_t offset = 0;
	for (size_t i = 0; i < sstables.size(); i++) {
		for (size_t j = 0; j < sstables.at(i).size(); j++) {
			offset = sstables.at(i).at(j)->get_offset(key);
			if (offset != 0) {
				return offset;
			}
		}
	}
	return 0;
}

void KVStore::compaction(uint32_t level)
{
	if (sstables[level].size() <= ((uint64_t)0x1 << (level + 1))) {
		return;
	}

	std::vector<SStable *> vec;
	std::vector<std::tuple<uint64_t, uint64_t, uint32_t>> tuples, tmp_tuples;
	uint64_t stamp = 0, tmp_stamp = 0;

	std::sort(sstables[level].begin(), sstables[level].end(), [](SStable *a, SStable *b) {
        return a->get_stamp() > b->get_stamp() || ((a->get_stamp() == b->get_stamp()) && (a->get_max_key() > b->get_max_key()));
    });
	if (level == 0) {
	 	std::swap(vec, sstables[level]);
	}
	else {
    	for (int i = sstables[level].size() - 1; i >= 0; i--) {
        	SStable *tmp_sstable = sstables[level][i];
        	vec.push_back(tmp_sstable);
        	sstables[level].erase(sstables[level].begin() + i);
        	if (sstables[level].size() <= ((uint64_t)0x1 << (level + 1))) {
            	break;
        	}
    	}
	}

	uint64_t min_key = vec[0]->get_min_key();
	uint64_t max_key = vec[0]->get_max_key(); 
	for (size_t i = 0; i < vec.size(); i++) {
	 	if (vec[i]->get_min_key() < min_key) {
	 		min_key = vec[i]->get_min_key();
	 	}
	 	if (vec[i]->get_max_key() > max_key) {
	 		max_key = vec[i]->get_max_key();
	 	}
	}

	if (level + 1 < sstables.size()) { 
		for (int i = sstables[level + 1].size() - 1; i >= 0; i--) {
	 		SStable *tmp_sstable = sstables[level + 1].at(i);
			if (tmp_sstable->get_max_key() < min_key || tmp_sstable->get_min_key() > max_key) {
				continue;
			}
			vec.push_back(tmp_sstable);
  	        sstables[level + 1].erase(sstables[level + 1].begin() + i);
		}
	}
	
	std::unordered_map<uint64_t, std::tuple<uint64_t, uint64_t, uint32_t>> mergedTuples;
	std::unordered_map<uint64_t, uint64_t> mergedStamp;
	for (auto& sstable : vec) {
        tmp_tuples = sstable->get_tuples();
        tmp_stamp = sstable->get_stamp();

        for (auto& tmp_tuple : tmp_tuples) {
            uint64_t tmp_key = std::get<0>(tmp_tuple);

            if (mergedTuples.find(tmp_key) == mergedTuples.end() || mergedStamp[tmp_key] < tmp_stamp) {
				mergedTuples[tmp_key] = tmp_tuple;
				mergedStamp[tmp_key] = tmp_stamp;
            }
        }

        if (tmp_stamp > stamp) {
            stamp = tmp_stamp;
        }
    }

	if (level == sstables.size() - 1) {
    	for (auto it = mergedTuples.begin(); it != mergedTuples.end();) {
        	if (std::get<2>(it->second) == 0) {
            	it = mergedTuples.erase(it);
        	} else {
            	++it;
        	}
    	}
	}

    for (auto& pair : mergedTuples) {
    	tuples.push_back(pair.second);
    }
	
	sort_tuples(tuples);

	if (level + 1 >= sstables.size()) {
		sstables.push_back(std::vector<SStable *>());
	}
	if (!dirExists(dir + "/level-" + std::to_string(level + 1))) {
		mkdir(dir + "/level-" + std::to_string(level + 1));
	}

	size_t batchSize = 408;
    for (size_t start = 0; start < tuples.size(); start += batchSize) {
        size_t end = std::min(start + batchSize, tuples.size());
        std::vector<std::tuple<uint64_t, uint64_t, uint32_t>> tmp_tuples(tuples.begin() + start, tuples.begin() + end);
        std::string newFilePath = dir + "/level-" + std::to_string(level + 1) + "/" + std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()) + ".sst";
        SStable* newSSTable = new SStable(stamp, newFilePath, tmp_tuples);
        sstables[level + 1].push_back(newSSTable);
    }

	for (auto* sstable : vec) {
        rmfile(sstable->get_path());
        delete sstable;
    }

	compaction(level + 1);
}

