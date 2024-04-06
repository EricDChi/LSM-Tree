#include "skiplist.h"
#include <optional>

SkipList::SkipList(double p) {
    header = new Node(0, "", max_level);
    level = 0;
    this->p = p;
}


void SkipList::put(key_type key, const value_type &val) {
    Node *current = header;
    std::vector<Node*> update = std::vector<Node*>(max_level + 1, nullptr);

    for (int i = level; i >= 0; i--) {
        while (current->next[i] != nullptr && current->next[i]->get_key() < key) {
            current = current->next[i];
        }
        update[i] = current;
    }

    current = current->next[0];

    if (current != nullptr && current->get_key() == key) {
        current->set_value(val);
    }
    if (current == nullptr || current->get_key() != key) {
        uint64_t v = randomlevel();
        if (v > level) {
            for (uint64_t i = level + 1; i < v + 1; i++) {
                update[i] = header;
            }
            level = v;
        } 
        
        Node *new_node = new Node(key, val, v);
        for (uint64_t i = 0; i < v + 1; i++) {
            new_node->next[i] = update[i]->next[i];
            update[i]->next[i] = new_node;
        } 
    }
}

Node* SkipList::find(key_type key) {
    Node *current = header;
    for(int i = level; i >= 0; i--) {
        while (current->next[i] != nullptr && current->next[i]->get_key() <= key) {
            current = current->next[i];
        }
    }
    if (current != nullptr && current->get_key() == key) {
        return current;
    }
    else {
        return nullptr;
    }
}

std::string SkipList::get(key_type key) const {
    Node *current = header;
    for(int i = level; i >= 0; i--) {
        while (current->next[i] != nullptr && current->next[i]->get_key() <= key) {
            current = current->next[i];
        }
    }
    if (current != nullptr && current->get_key() == key) {
        return current->get_value();
    }
    else {
        return "";
    }
}

void SkipList::del(key_type key) {
    Node *current = header;
    std::vector<Node*> update = std::vector<Node*>(max_level + 1, nullptr);

    for (int i = level; i >= 0; i--) {
        while (current->next[i] != nullptr && current->next[i]->get_key() < key) {
            current = current->next[i];
        }
        update[i] = current;
    }

    current = current->next[0];

    if (current != nullptr && current->get_key() == key) {
        for (uint64_t i = 0; i < level + 1; i++) {
            if (update[i]->next[i] != current) {
                break;
            }
            update[i]->next[i] = current->next[i];
        }
        while (level > 0 && header->next[level] == nullptr) {
            level--;
        }
        delete current;
    }
}

void SkipList::clear() {
    Node *current = header->next[0];
    while (current != nullptr) {
        Node *temp = current;
        current = current->next[0];
        delete temp;
    }
    for (int i = 0; i < max_level; i++) {
        header->next[i] = nullptr;
    }
    level = 0;
}

uint64_t SkipList::randomlevel() {
    uint64_t level = 0;
    while (rand() * 1.0 / RAND_MAX <= p) {
        level++;
        if (level == max_level) {
            break;
        }
    }
    return level;
}

void SkipList::get_data(std::vector<std::pair<uint64_t, std::string>> &data) {
    Node* node = header;
    while (node != nullptr) {
        data.push_back({node->get_key(), node->get_value()});
        node = node->next[0];
    }
}

void SkipList::scan(uint64_t key1, uint64_t key2, std::list<std::pair<uint64_t, std::string>> &list) {
    Node* node = header;
    uint64_t key;
    std::string val;
    while (node != nullptr) {
        key = node->get_key();
        if (key >= key1 && key <= key2) {
            val = node->get_value();
            list.push_back({key, val});
        }
        node = node->next[0];
    }
}