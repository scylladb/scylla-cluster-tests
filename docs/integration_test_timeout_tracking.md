# Integration Test Timeout Tracking

Integration tests were timing out without clear indication which test or which phase (setup/call/teardown) was stuck. Added `pytest-timeout==2.3.1` for 1-hour per-test timeout enforcement and pytest hooks in `unit_tests/conftest.py` to log each phase of integration test execution with timestamps. Logs show test name, phase, setup duration, call duration, and total time - making it easy to identify stuck tests in Jenkins console logs.

Example output shows which test is running and where it's spending time:
```
INFO [INTEGRATION TEST START] unit_tests/test_gemini_thread.py::test_01_gemini_thread
INFO [INTEGRATION TEST CALL] unit_tests/test_gemini_thread.py::test_01_gemini_thread (setup took 15.34s)
INFO [INTEGRATION TEST TEARDOWN] unit_tests/test_gemini_thread.py::test_01_gemini_thread (total runtime so far: 125.67s)
INFO [INTEGRATION TEST PASSED] unit_tests/test_gemini_thread.py::test_01_gemini_thread (call duration: 110.33s)
INFO [INTEGRATION TEST COMPLETE] unit_tests/test_gemini_thread.py::test_01_gemini_thread (total time: 125.67s)
```
