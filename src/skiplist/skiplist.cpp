#include "skiplist.h"

namespace LSMT {
/*** SkipListNode Implementation ***/
SkipListNode::SkipListNode(const std::string &k, const std::string& v, int level, uint64_t trx_id)
     : key(k), val(v), next(level, nullptr), trx_id(trx_id) { }

bool SkipListNode::operator<(const SkipListNode &other) const {
     return key < other.key || (key == other.key && trx_id > other.trx_id);
}

bool SkipListNode::operator>(const SkipListNode &other) const {
     return key > other.key || (key == other.key && trx_id < other.trx_id);
}

bool SkipListNode::operator==(const SkipListNode &other) const {
     return key == other.key && val == other.val && trx_id == other.trx_id;
}

bool SkipListNode::operator!=(const SkipListNode &other) const {
     return key != other.key || val != other.val || trx_id != other.trx_id;
}


/*** SkipList Implementation ***/
SkipList::SkipList(int max_level) : max_level(max_level), cur_level(1) {
     head = std::make_shared<SkipListNode>("", "", max_level, 0);
     gen = std::mt19937(std::random_device()());
     dis_binomary = std::uniform_int_distribution<>(0, 1);
}

SkipList::~SkipList() { }

int SkipList::random_level() {
     int level = 1;
     while (dis_binomary(gen) && level < max_level) { level++; }
     return level;
}

void SkipList::put(const std::string &key, const std::string &val, uint64_t trx_id) {
     std::vector<std::shared_ptr<SkipListNode>> update(max_level, nullptr);
     int new_level = random_level();
     auto new_node = std::make_shared<SkipListNode>(key, val, new_level, trx_id);
     
     auto current = this->head;
     for (int i = cur_level - 1; i >= 0; --i) {
          while (current->next[i] && *current->next[i] < *new_node) {
               current = current->next[i];
          }
          update[i] = current;
     }

     current = current->next[0];
     if (current && current->key == key && current->trx_id == trx_id) {
          size_bytes += val.size() - current->val.size();
          current->val = val; 
          current->trx_id = trx_id;
     } else {
          if (new_level > cur_level) {
               for (int i = cur_level; i < new_level; ++i) { update[i] = head; }
          }
          for (int i = 0; i < new_level; ++i) {
               new_node->next[i] = update[i]->next[i];
               update[i]->next[i] = new_node;
          }
          size_bytes += key.size() + val.size() + sizeof(uint64_t);
          cur_level = std::max(cur_level, new_level);
     }
}

SkipListIterator SkipList::get(const std::string &key, uint64_t trx_id) {
     auto current = head;
     for (int i = cur_level - 1; i >= 0; --i) {
          while (current->next[i] && current->next[i]->key < key) {
               current = current->next[i];
          }
     }

     current = current->next[0];
     if (trx_id == 0) {
          if (current && current->key == key) {
               return SkipListIterator(current);
          }
     } else {
          while (current && current->key == key) {
               if (current->trx_id <= trx_id) {
                    return SkipListIterator(current);
               } else {
                    current = current->next[0];
               }
          }
     }
     return SkipListIterator();
}

void SkipList::remove(const std::string &key) {
     std::vector<std::shared_ptr<SkipListNode>> update(max_level, nullptr);

     auto current = head;
     for (int i = cur_level - 1; i >= 0; --i) {
          while (current->next[i] && current->next[i]->key < key) {
               current = current->next[i];
          }
          update[i] = current;
     }

     current = current->next[0];
     if (current->key == key) {
          for (int i = cur_level - 1; i >= 0; --i) {
               if (update[i]->next[i] != current) { continue; }
               update[i]->next[i] = current->next[i];
          }
          size_bytes -= current->key.size() + current->val.size() + sizeof(uint64_t);
          while (cur_level > 1 && !head->next[cur_level - 1]) {
               --cur_level;
          }
     }
}

std::vector<std::tuple<std::string, std::string, uint64_t>> SkipList::flush() {
     std::vector<std::tuple<std::string, std::string, uint64_t>> data;
     auto current = head->next[0];
     while (current) {
          data.emplace_back(current->key, current->val, current->trx_id);
          current = current->next[0];
     }
     return data;
}

void SkipList::clear() {
     head = std::make_shared<SkipListNode>("", "", max_level, 0);
     size_bytes = 0;
}

size_t SkipList::get_size() { return size_bytes; }

SkipListIterator SkipList::begin() {
     return SkipListIterator(head->next[0]);
}

SkipListIterator SkipList::end() {
     return SkipListIterator();
}

SkipListIterator SkipList::begin_preffix(const std::string &preffix) {
     auto current = head;
     for (int i = cur_level - 1; i >= 0; --i) {
          while (current->next[i] && current->next[i]->key < preffix) {
               current = current->next[i];
          }
     }
     current = current->next[0];
     return SkipListIterator(current);
}

SkipListIterator SkipList::end_preffix(const std::string &preffix) {
     auto current = head;
     for (int i = cur_level - 1; i >= 0; --i) {
          while (current->next[i] && current->next[i]->key < preffix) {
               current = current->next[i];
          }
     }
     current = current->next[0];
     while (current && current->key.substr(0, preffix.size()) == preffix) {
          current = current->next[0];
     }
     return SkipListIterator(current);
}

std::optional<std::pair<SkipListIterator, SkipListIterator>> 
SkipList::iters_monotony_predicate(std::function<int(const std::string &)> predicate) {
     // 获取符合predicate谓词条件的起始迭代器
     auto current = head;
     for (int i = cur_level; i >= 0; --i) {
          while (current->next[i]) {
               auto direction = predicate(current->next[i]->key);
               if (direction <= 0) {
                    break;
               } else {
                    current = current->next[i];
               }
          }
     }
     if (predicate(current->next[0]->key) != 0) { 
          return std::nullopt; 
     }
     SkipListIterator beg_iter = SkipListIterator(current->next[0]);

     // 获取符合predicate谓词条件的末尾迭代器
     for (int i = current->next.size() - 1; i >= 0; --i) {
          while (current->next[i]) {
               auto direction = predicate(current->next[i]->key);
               if (direction == 0) {
                    current = current->next[i];
                    continue;
               } else if (direction < 0) {
                    break;
               } else if (direction > 0) {
                    throw std::runtime_error("Iters_predicate: invalid direction");
               }
          }
     }
     SkipListIterator end_iter = SkipListIterator(current->next[0]);

     return std::make_optional<std::pair<SkipListIterator, SkipListIterator>>(beg_iter, end_iter);
}
}  // LOG STRUCT MERGE TREE
