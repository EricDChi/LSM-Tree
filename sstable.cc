#include "sstable.h"
#include "utils.h"
#include "fstream"

SStable::SStable(uint64_t SST_timeStamp, std::string path, std::vector<std::pair<uint64_t, std::string>> &vector) {
    bloomfilter = new BloomFilter(bloomfilter_size * 8, 4);
    uint64_t key;
    std::string val;
    off64_t offset;
    uint32_t vlen;
    timeStamp = SST_timeStamp;
    key_num = vector.size();
    min_key = max_key = vector.back().first;

    std::fstream sst_file(path, std::ios::out|std::ios::binary);

    if (sst_file) {
        sst_file.seekg(header_size + bloomfilter_size, std::ios::beg);
        
        for (std::vector<std::pair<uint64_t, std::string>>::iterator iter = vector.begin(); iter!=vector.end(); iter++) {
            key = iter->first;
            val = iter->second;
            vlen = sizeof(val);
            key_num++;
            if (key < min_key) min_key = key;
            if (key > max_key) max_key = key;
            this->bloomfilter->add(key);
            
            put_vlog(key, vlen, val, offset);
            
            sst_file.write((char*)key, sizeof(key));
            sst_file.write((char*)offset, sizeof(offset));
            sst_file.write((char*)vlen, sizeof(vlen));
            auto tuple = std::make_tuple(key, offset, vlen);
            tuples.push_back(tuple);
        }
        sst_file.seekp(0, std::ios::beg);
        sst_file.write((char*)timeStamp, sizeof(SST_timeStamp));
        sst_file.write((char*)key_num, sizeof(key_num));
        sst_file.write((char*)min_key, sizeof(min_key));
        sst_file.write((char*)max_key, sizeof(max_key));
        this->bloomfilter->writeToFile(path);

        sst_file.close();
    }
}

SStable::SStable(std::string fullPath){
    uint64_t key;
    off64_t offset;
    uint32_t vlen;
    bloomfilter = new BloomFilter(bloomfilter_size * 8, 4);
    std::ifstream infile (path, std::ios::in|std::ios::binary);
    
    if (infile) {
        infile.seekg(0, std::ios::end);

        infile.read(reinterpret_cast<char*>(&timeStamp), sizeof(timeStamp));
        infile.read(reinterpret_cast<char*>(&key_num), sizeof(key_num));
        infile.read(reinterpret_cast<char*>(&min_key), sizeof(min_key));
        infile.read(reinterpret_cast<char*>(&max_key), sizeof(max_key));
        
        bloomfilter->readFromFile(fullPath);

        while(infile.tellg() != infile.end) {
            infile.read(reinterpret_cast<char*>(&key), sizeof(key));
            infile.read(reinterpret_cast<char*>(&offset), sizeof(offset));
            infile.read(reinterpret_cast<char*>(&vlen), sizeof(vlen));
            auto tuple = std::make_tuple(key, offset, vlen);
            tuples.push_back(tuple);
        }
    } 
}

SStable::~SStable() {
    delete bloomfilter;
    tuples.clear();
}

void SStable::put_vlog(uint64_t key, uint32_t vlen, std::string &s, off64_t &offset) {
    uint16_t Checksum;
    std::string data;

    std::fstream vlog_file(vlog_path, std::ios::in|std::ios::out|std::ios::binary);

    if (vlog_file) {
        vlog_file.seekp(0, std::ios::end);
        offset = vlog_file.tellp() + sizeof(Magic) + sizeof(uint16_t) + sizeof(uint64_t) + sizeof(uint32_t);

        data = std::to_string(key) + s + std::to_string(vlen);
		Checksum = utils::crc16(std::vector<unsigned char>(data.begin(), data.end()));data.push_back(key);

        vlog_file.seekp(offset, std::ios::beg);
        vlog_file.write((char*)Magic, sizeof(Magic));
        vlog_file.write((char*)Checksum, sizeof(uint16_t));
        vlog_file.write((char*)key, sizeof(uint64_t));
        vlog_file.write((char*)vlen, sizeof(uint32_t));
        vlog_file.write(s.c_str(), sizeof(s));
    }
}

std::string SStable::get(uint64_t key) {
    std::string val;
    if (!this->bloomfilter->check(key)) {
        return "";
    }
    if (key > max_key || key < min_key) {
        return "";
    }
    for (int i = 0; i < tuples.size(); i++) {
        uint64_t cur_key = std::get<0>(tuples.at(i));
        if (cur_key == key) {
            std::fstream infile(vlog_path, std::ios::in|std::ios::binary);
            off64_t offset = std::get<1>(tuples.at(i));
            uint32_t vlen = std::get<2>(tuples.at(i));
            char c[vlen];
            if (infile) {
                infile.seekg(offset,infile.beg);
                infile.read(c, vlen);
                infile.close();
            }
            strcpy(c, val.c_str());
            return val;
        }
    }
}

void SStable::scan(uint64_t key1, uint64_t key2, std::list<std::pair<uint64_t, std::string>> &list) {
    std::string val;
    for (int i = 0; i < tuples.size(); i++) {
        uint64_t cur_key = std::get<0>(tuples.at(i));
        if (cur_key >= key1 && cur_key <= key2) {
            std::fstream infile(vlog_path, std::ios::in|std::ios::binary);
            off64_t offset = std::get<1>(tuples.at(i));
            uint32_t vlen = std::get<2>(tuples.at(i));
            char c[vlen];
            if (infile) {
                infile.seekg(offset,infile.beg);
                infile.read(c, vlen);
                infile.close();
            }
            strcpy(c, val.c_str());
            list.push_back({cur_key, val});
        }
    }
}