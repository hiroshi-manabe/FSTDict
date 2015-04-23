cmake_minimum_required(VERSION 3.2)
add_executable(fst_test fst_test.cpp)
target_compile_features(fst_test PRIVATE cxx_constexpr)
