#include "fst.h"

#include <iostream>
#include <string>
#include <vector>

using namespace std;

void TestFSTCommonPrefixSearch01() {
  vector<FstDict::Pair> inp {
    {"こんにちは", 111},
    {"世界", 222},
    {"すもももももも", 333},
    {"すもも", 333},
    {"すもも", 444},
  };
  string err;
  auto vm = BuildFST(&inp, &err);
  cout << vm->toString();
}

int main(void) {
  TestFSTCommonPrefixSearch01();
  return 0;
}
