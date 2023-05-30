## Setup SCT locally development environment

This is the recommend why of building a local development environment
that isn't depended on the distro python version.

To run SCT tests locally run following::

```bash
sudo ./install-prereqs.sh
./get-qa-ssh-keys.sh

# install python3.10 via pyenv
curl https://pyenv.run | bash
exec $SHELL
# go to: https://github.com/pyenv/pyenv/wiki/Common-build-problems#prerequisites
# and follow the instructions for your distribution, to install the prerequisites
# for compiling python from source
pyenv install 3.10.0

# create a virtualenv for SCT
pyenv virtualenv 3.10.0 sct310
pyenv activate sct310
pip install -r requirements.in
```

### SCT test profiling
-
- set environment variable "SCT_ENABLE_TEST_PROFILING" to 1, or add "enable_test_profiling: true" into yaml file
- run test

After test is done there are following ways to use collected stats:
- `cat ~/latest/profile.stats/stats.txt`
- `snakeviz ~/latest/profile.stats/stats.bin`
- `tuna ~/latest/profile.stats/stats.bin`
- `gprof2dot -f pstats ~/latest/profile.stats/stats.bin | dot -Tpng -o ~/latest/profile.stats/stats.png`

Another way to profile is py-spy:
- `pip install py-spy`
Run recording:
- `py-spy record -s -o ./py-spy.svg -- python3 sct.py ...`
Run 'top' mode:
- `py-spy top -s -- python3 sct.py ...`
