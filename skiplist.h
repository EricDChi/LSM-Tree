#pragma once

#include <cstdint>
#include <string>
#include <vector>
#include <cstring>
#include <iostream>
#include <list>

const int max_level = 20;

using key_type = uint64_t;
// using value_type = std::vector<char>;
using value_type = std::string;

class Node 
{
private:
	key_type key;
	value_type val;
	int node_level;

public:
	Node() {};
	Node(key_type k, value_type v, int l)
	:key(k), val(v), node_level(l), next(21, nullptr) {}

	value_type get_value() {return val;};

	key_type get_key() {return key;};

	void set_value(value_type val) {this->val = val;};

	std::vector<Node*> next;
};

class SkipList
{
	// add something here
private:
	Node *header;
	double p;
	uint64_t level;

public:
	SkipList();
	SkipList(double p);

	~SkipList();

	void put(key_type key, const value_type &val);

	void del(key_type key);

	void clear();

	Node* find(key_type key);

	std::string get(key_type key) const;
	
	void get_data(std::vector<std::pair<uint64_t, std::string>> &data);

	void scan(uint64_t key1, uint64_t key2, std::list<std::pair<uint64_t, std::string>> &list);

	uint64_t randomlevel();
};

