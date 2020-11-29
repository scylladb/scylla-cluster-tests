# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as published by
# the Free Software Foundation; either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
#
# See LICENSE for more details.
#
# Copyright (c) 2020 ScyllaDB

import os

from sdcm.remote import shell_script_cmd
from sdcm.tester import ClusterTester, teardown_on_exception
from sdcm.utils.decorators import log_run_info


DB_SSH_KEY = "db_node_ssh_key"


class JepsenTest(ClusterTester):
    @property
    def jepsen_node(self):
        return self.loaders.nodes[0]

    @teardown_on_exception
    @log_run_info
    def setup_jepsen(self):
        remoter = self.jepsen_node.remoter
        remoter.sudo("apt-get install -y libjna-java gnuplot graphviz git")
        remoter.run(shell_script_cmd(f"""\
            curl -O https://raw.githubusercontent.com/technomancy/leiningen/stable/bin/lein
            chmod +x lein
            ./lein
            git clone {self.params.get('jepsen_scylla_repo')} jepsen-scylla
        """))
        for db_node in self.db_cluster.nodes:
            remoter.run(f"ssh-keyscan -t rsa {db_node.ip_address} >> ~/.ssh/known_hosts")
        remoter.send_files(os.path.expanduser(self.db_cluster.nodes[0].ssh_login_info["key_file"]), DB_SSH_KEY)

    def setUp(self):
        super().setUp()
        self.setup_jepsen()

    def test_jepsen(self):
        tests = self.params.get('jepsen_test_cmd')
        nodes = " ".join(f"--node {node.ip_address}" for node in self.db_cluster.nodes)
        creds = f"--username {self.db_cluster.nodes[0].ssh_login_info['user']} --ssh-private-key ~/{DB_SSH_KEY}"
        jepsen_cmd = f"cd ~/jepsen-scylla && ~/lein run {tests} {nodes} {creds}"

        self.log.info("Run Jepsen test: `%s'", jepsen_cmd)
        self.jepsen_node.remoter.run(jepsen_cmd)
