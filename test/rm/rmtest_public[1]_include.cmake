if(EXISTS "/mnt/c/Users/sehwanhong/CLionProjects/cs222-fall20-team-11/test/rm/rmtest_public[1]_tests.cmake")
  include("/mnt/c/Users/sehwanhong/CLionProjects/cs222-fall20-team-11/test/rm/rmtest_public[1]_tests.cmake")
else()
  add_test(rmtest_public_NOT_BUILT rmtest_public_NOT_BUILT)
endif()
