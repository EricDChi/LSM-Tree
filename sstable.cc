#include "sstable.h"
#include "utils.h"
#include <fstream>

SStable::SStable(uint64_t SST_timeStamp, std::string filePath, std::string vlog, std::vector<std::pair<uint64_t, std::string>> &vector) {
    bloomfilter = new BloomFilter(bloomfilter_size * 8, 4);
    uint64_t key;
    std::string val;
    uint64_t offset;
    uint32_t vlen;
    stamp = SST_timeStamp;
    key_num = vector.size();
    min_key = max_key = vector.back().first;
    path = filePath;

    std::fstream sst_file(path, std::ios::out|std::ios::binary);

    if (sst_file) {
        sst_file.seekp(header_size + bloomfilter_size, std::ios::beg);
        
        for (auto &entry : vector) {
            key = entry.first;
            val = entry.second;
            vlen = val.size(); // Correct size calculation for string length
            if (key < min_key) min_key = key;
            if (key > max_key) max_key = key;
            this->bloomfilter->add(key);
            if (val == "~DELETE~") {
                offset = 1;
                vlen = 0;
                sst_file.write(reinterpret_cast<char*>(&key), sizeof(key));
                sst_file.write(reinterpret_cast<char*>(&offset), sizeof(offset));
                sst_file.write(reinterpret_cast<char*>(&vlen), sizeof(vlen));
                auto tuple = std::make_tuple(key, offset, vlen);
                tuples.push_back(tuple);
                continue;
            }

            put_vlog(vlog, key, vlen, val, offset);
            
            sst_file.write(reinterpret_cast<char*>(&key), sizeof(key));
            sst_file.write(reinterpret_cast<char*>(&offset), sizeof(offset));
            sst_file.write(reinterpret_cast<char*>(&vlen), sizeof(vlen));
            auto tuple = std::make_tuple(key, offset, vlen);
            tuples.push_back(tuple);
        }
        sst_file.seekp(0, std::ios::beg);
        sst_file.write(reinterpret_cast<char*>(&stamp), sizeof(stamp));
        sst_file.write(reinterpret_cast<char*>(&key_num), sizeof(key_num));
        sst_file.write(reinterpret_cast<char*>(&min_key), sizeof(min_key));
        sst_file.write(reinterpret_cast<char*>(&max_key), sizeof(max_key));
        this->bloomfilter->writeToFile(sst_file);

        sst_file.close();
    }
}

SStable::SStable(uint64_t SST_timeStamp, std::string filePath, std::vector<std::tuple<uint64_t, uint64_t, uint32_t>> tmp_tuples) {
    bloomfilter = new BloomFilter(bloomfilter_size * 8, 4);
    uint64_t key;
    uint64_t offset;
    uint32_t vlen;
    stamp = SST_timeStamp;
    key_num = tuples.size();
    tuples = tmp_tuples;
    min_key = max_key = std::get<0>(tuples.back());
    path = filePath;

    std::fstream sst_file(path, std::ios::out|std::ios::binary);

    if (sst_file) {
        sst_file.seekp(header_size + bloomfilter_size, std::ios::beg);
        
        for (auto &entry : tuples) {
            key = std::get<0>(entry);
            offset = std::get<1>(entry);
            vlen = std::get<2>(entry);
            if (key < min_key) min_key = key;
            if (key > max_key) max_key = key;
            this->bloomfilter->add(key);
            
            sst_file.write(reinterpret_cast<char*>(&key), sizeof(key));
            sst_file.write(reinterpret_cast<char*>(&offset), sizeof(offset));
            sst_file.write(reinterpret_cast<char*>(&vlen), sizeof(vlen));
        }
        sst_file.seekp(0, std::ios::beg);
        sst_file.write(reinterpret_cast<char*>(&stamp), sizeof(stamp));
        sst_file.write(reinterpret_cast<char*>(&key_num), sizeof(key_num));
        sst_file.write(reinterpret_cast<char*>(&min_key), sizeof(min_key));
        sst_file.write(reinterpret_cast<char*>(&max_key), sizeof(max_key));
        this->bloomfilter->writeToFile(sst_file);

        sst_file.close();
    }
}

SStable::SStable(std::string fullPath){
    uint64_t key;
    uint64_t offset;
    uint32_t vlen;
    bool *bit = new bool[bloomfilter_size * 8];
    path = fullPath;
    std::ifstream infile(fullPath, std::ios::binary);

    if (infile) {
        infile.read(reinterpret_cast<char*>(&stamp), sizeof(stamp));
        infile.read(reinterpret_cast<char*>(&key_num), sizeof(key_num));
        infile.read(reinterpret_cast<char*>(&min_key), sizeof(min_key));
        infile.read(reinterpret_cast<char*>(&max_key), sizeof(max_key));

        char buffer;
        int index = 0;
        while (infile.read(&buffer, 1)) {
            for (int i = 0; i < 8; ++i) {
                bit[index++] = (buffer & (1 << i)) != 0;
            }
            if (index >= bloomfilter_size * 8) break;
        }
        this->bloomfilter = new BloomFilter(bloomfilter_size * 8, 4, bit);

        // 读取剩余的数据，直到到达文件结尾
        while(infile.good()) {
            infile.read(reinterpret_cast<char*>(&key), sizeof(key));
            if (!infile.good()) break;  // 检查读取key后的状态
            infile.read(reinterpret_cast<char*>(&offset), sizeof(offset));
            if (!infile.good()) break;  // 检查读取offset后的状态
            infile.read(reinterpret_cast<char*>(&vlen), sizeof(vlen));
            if (!infile.good()) break;  // 检查读取vlen后的状态
                
            tuples.push_back(std::make_tuple(key, offset, vlen));
        }
        if (!infile.eof()) {
            std::cerr << "File read error before reaching the end." << std::endl;
        }

        infile.close();
    } else {
        std::cerr << "Failed to open the file: " << fullPath << std::endl;
    }
}

SStable::~SStable() {
    delete bloomfilter;
    tuples.clear();
}

void SStable::put_vlog(std::string vlog, uint64_t key, uint32_t vlen, std::string &s, uint64_t &offset) {
    uint16_t Checksum;
    std::string data;

    std::fstream vlog_file(vlog, std::ios::in | std::ios::out | std::ios::binary | std::ios::app);
    if (vlog_file) {   
        vlog_file.seekp(0, std::ios::end);
        data = std::to_string(key) + s + std::to_string(vlen);
        Checksum = utils::crc16(std::vector<unsigned char>(data.begin(), data.end()));
        uint8_t magic = Magic;
        vlog_file.write((char*)&magic, sizeof(magic));
        vlog_file.write((char*)&Checksum, sizeof(Checksum));
        vlog_file.write((char*)&key, sizeof(key));
        vlog_file.write((char*)&vlen, sizeof(vlen));
        offset = static_cast<uint64_t>(vlog_file.tellp());
        vlog_file.write(s.c_str(), s.size());

        vlog_file.close();
    }
}

std::string SStable::get(uint64_t key, std::string vlog) {
    std::tuple <uint64_t, uint64_t, uint32_t> tuple = get_tuple(key);
    if (tuple != std::make_tuple(0, 0, 0)) {
        std::fstream infile(vlog, std::ios::in | std::ios::binary);
        uint64_t offset = std::get<1>(tuple);
        uint32_t vlen = std::get<2>(tuple);
        if (vlen == 0) return "~DELETE~";
        std::vector<char> buffer(vlen);
        if (infile) {
            infile.seekg(offset);
            infile.read(buffer.data(), vlen);
            infile.close();
        }
        std::string val(buffer.begin(), buffer.end());
        return val;
    }
    return "";
}

std::string SStable::get_without_bloomfilter(uint64_t key, std::string vlog) {
    std::tuple <uint64_t, uint64_t, uint32_t> tuple = get_tuple(key);
    if (tuple != std::make_tuple(0, 0, 0)) {
        std::fstream infile(vlog, std::ios::in | std::ios::binary);
        uint64_t offset = std::get<1>(tuple);
        uint32_t vlen = std::get<2>(tuple);
        if (vlen == 0) return "~DELETE~";
        std::vector<char> buffer(vlen);
        if (infile) {
            infile.seekg(offset);
            infile.read(buffer.data(), vlen);
            infile.close();
        }
        std::string val(buffer.begin(), buffer.end());
        return val;
    }
    return "";
}

void SStable::scan(std::string vlog, uint64_t key1, uint64_t key2, std::list<std::pair<uint64_t, std::string>> &list) {
    for (const auto &tuple : tuples) {
        uint64_t cur_key = std::get<0>(tuple);
        if (cur_key >= key1 && cur_key <= key2) {
            // 检查list中是否已存在此key
            bool keyExists = false;
            for (const auto &item : list) {
                if (item.first == cur_key) {
                    keyExists = true;
                    break;
                }
            }
            if (!keyExists) {
                std::fstream infile(vlog, std::ios::in | std::ios::binary);
                uint64_t offset = std::get<1>(tuple);
                uint32_t vlen = std::get<2>(tuple);
                std::vector<char> buffer(vlen);
                if (infile) {
                    infile.seekg(offset);
                    infile.read(buffer.data(), vlen);
                    infile.close();
                }
                std::string val(buffer.begin(), buffer.end());
                list.push_back({cur_key, val});
            }
        }
    }
}

uint64_t SStable::get_offset(uint64_t key) {
    std::tuple <uint64_t, uint64_t, uint32_t> tuple = get_tuple(key);
    if (tuple != std::make_tuple(0, 0, 0)) {
        return std::get<1>(tuple);
    }
    return 0;
}

std::tuple<uint64_t, uint64_t, uint32_t> SStable::get_tuple(uint64_t key) {
    if (!bloomfilter->check(key)) {
        return std::make_tuple(0, 0, 0);
    }
    if (key > max_key || key < min_key) {
        return std::make_tuple(0, 0, 0);
    }

    int left = 0;
    int right = tuples.size() - 1;
    while (left <= right) {
        int mid = left + (right - left) / 2;
        if (std::get<0>(tuples[mid]) == key) {
            return tuples[mid];
        }
        if (std::get<0>(tuples[mid]) < key) {
            left = mid + 1;
        } else {
            right = mid - 1;
        }
    }
    return std::make_tuple(0, 0, 0);
}

std::tuple<uint64_t, uint64_t, uint32_t> SStable::get_tuple_without_bloomfilter(uint64_t key) {
    int left = 0;
    int right = tuples.size() - 1;
    while (left <= right) {
        int mid = left + (right - left) / 2;
        if (std::get<0>(tuples[mid]) == key) {
            return tuples[mid];
        }
        if (std::get<0>(tuples[mid]) < key) {
            left = mid + 1;
        } else {
            right = mid - 1;
        }
    }
    return std::make_tuple(0, 0, 0);
}