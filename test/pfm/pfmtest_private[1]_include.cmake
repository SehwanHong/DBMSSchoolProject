if(EXISTS "/mnt/c/Users/sehwanhong/CLionProjects/cs222-fall20-team-11/test/pfm/pfmtest_private[1]_tests.cmake")
  include("/mnt/c/Users/sehwanhong/CLionProjects/cs222-fall20-team-11/test/pfm/pfmtest_private[1]_tests.cmake")
else()
  add_test(pfmtest_private_NOT_BUILT pfmtest_private_NOT_BUILT)
endif()
