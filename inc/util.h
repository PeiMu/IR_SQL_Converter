#pragma once

#include <iostream>
#include <memory>

namespace ir_sql_converter {

using idx_t = unsigned int;

template <typename T, typename S>
std::unique_ptr<S> unique_ptr_cast(std::unique_ptr<T> src) {
  return std::unique_ptr<S>(static_cast<S *>(src.release()));
}

struct pair_hash {
  template <class T1, class T2>
  idx_t operator()(const std::pair<T1, T2> &p) const {
    auto h1 = std::hash<T1>{}(p.first);
    auto h2 = std::hash<T2>{}(p.second);

    // Mainly for demonstration purposes, i.e. works but is overly simple
    // In the real world, use sth. like boost.hash_combine
    return h1 ^ h2;
  }
};
} // namespace ir_sql_converter
