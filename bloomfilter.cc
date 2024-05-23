#include "bloomfilter.h"

BloomFilter::BloomFilter(int m, int k) {
    this->bit_count = m;
    this->hash_count = k;
    bit = new bool[bit_count];
    hash = new unsigned int[hash_count];
}

BloomFilter::BloomFilter(int m, int k, bool *bits) {
    this->bit_count = m;
    this->hash_count = k;
    this->bit = new bool[bit_count];
    std::copy(bits, bits + bit_count, bit);
    hash = new unsigned int[hash_count];
}

BloomFilter::~BloomFilter() {
    delete [] bit;
    delete [] hash; 
}

void BloomFilter::add(uint64_t key) {
    MurmurHash3_x64_128(&key, sizeof(key), 0, hash);
    for (int i = 1; i <= (hash_count - 1) / 4; i++) {
        MurmurHash3_x64_128(&key, sizeof(key), *(hash + 4 * (i - 1)), hash + 4 * i) ;
    }

    for(int i = 0; i < hash_count; i++) {
        bit[hash[i] % bit_count] = true;
    }
}

bool BloomFilter::check(uint64_t key) {
    MurmurHash3_x64_128(&key, sizeof(key), 0, hash);
    for (int i = 1; i <= (hash_count - 1) / 4; i++) {
        MurmurHash3_x64_128(&key, sizeof(key), *(hash + 4 * (i - 1)), hash + 4 * i) ;
    }
    for(int i = 0; i < hash_count; i++) {
        if(!bit[hash[i] % bit_count]) {
            return false;
        }
    }
    return true;
}

void BloomFilter::writeToFile(std::fstream &sst_file) {
    if (sst_file) {
        sst_file.seekp(header_size, std::ios::beg);
        char buffer;
        for (int i = 0; i < bloomfilter_size * 8; i++) {
            if (i % 8 == 0 && i != 0) {
                sst_file.write(&buffer, 1);
                buffer = 0;
            }
            if (bit[i]) {
                buffer |= (1 << (i % 8));
            }
        }
        sst_file.write(&buffer, 1);  // write the last buffer
        sst_file.close();
    }
}
