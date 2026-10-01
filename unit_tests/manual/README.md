# Manual tests

Tests in this directory need real infrastructure (SSH access, cloud accounts, Docker with network access) and are
skipped by `uv run sct.py unit-tests` and `uv run sct.py integration-tests`. Run them by hand with `uv run pytest`.

| File | Needs | How to run |
|------|-------|------------|
| `test_manual.py` | Depends on the class (stress tools, AWS credentials) | Remove the `skip` mark of the class and run `uv run pytest unit_tests/manual/test_manual.py -k <ClassName>` |
| `test_remoter.py` | `~/.ssh/scylla_test_id_ed25519` listed in the user's `~/.ssh/authorized_keys`, local sshd | Remove the `skip` mark of the test and run `uv run pytest unit_tests/manual/test_remoter.py -k <test_name>` |
| `test_vector_install_script.py` | Docker, network access | `uv run pytest unit_tests/manual/test_vector_install_script.py -p no:skipping` |
| `test_kernel_panic.py` | Cloud credentials for the backend, `SCT_TEST_KERNEL_PANIC=1` | `SCT_TEST_KERNEL_PANIC=1 uv run pytest unit_tests/manual/test_kernel_panic.py -v -s` |
| `test_azure_vm_provider_reboot.py` | Azure credentials, `SCT_TEST_AZURE_REBOOT=1` | `SCT_TEST_AZURE_REBOOT=1 uv run pytest unit_tests/manual/test_azure_vm_provider_reboot.py -v -s` |
