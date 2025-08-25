import time

import pytest
from cassandra import Unauthorized

from longevity_test import LongevityTest
from sdcm.cluster import BaseNode
from sdcm.sct_events.system import InfoEvent
from sdcm.utils.ldap import LdapServerType, LDAP_USERS, LDAP_PASSWORD, LdapUtilsMixin


class AddRemoveLdapRolePermissionTest(LongevityTest, LdapUtilsMixin):

    def test_add_remove_ldap_role_permission(self):
        """
        Test adding a new user with Ldap permissions,
        and run some load for it.
        Then remove permissions and wait for its load to end.
        Scenario :
        1. Create new user in Scylla
        2. Create new role in Ldap containing the new user as a member.
        3. Verify this new user is unauthorised on resources (create a keyspace).
        4. Create a new super-user role in Scylla.
        5. Create a new super-user role in Ldap, with the new user as a member.
        6. Verify this new user is authorised on resources.
        7. Modify Ldap role to remove membership of new user.
        8. Verify the new user is unauthorised on resources (create a table).
        9. Do clean-up: delete Ldap Role, Scylla roles and keyspace/tables.

        """
        node: BaseNode = self.db_cluster.nodes[0]
        stress_queue = []
        background_write_cmd = self.params.get('stress_cmd')
        InfoEvent(message=f"Starting C-S stress load: {background_write_cmd}").publish()
        stress_queue.append(self.run_stress_thread(stress_cmd=background_write_cmd, round_robin=True))

        if not node.is_enterprise:
            raise ValueError('Cluster is not enterprise. LDAP is supported only for enterprise. aborting.')
        if self.params.get('use_ldap_authentication'):
            raise ValueError('Skipping this test with use_ldap_authentication because of scylla-enterprise#2641')
        if not self.params.get('use_ldap_authorization'):
            raise ValueError('Cluster is not configured to run with LDAP authorization, aborting.')
        if self.params.get('ldap_server_type') == LdapServerType.MS_AD:
            raise ValueError('Cluster is not configured to run with open LDAP authorization, aborting.')
        superuser_role = 'superuser_role'
        new_test_user = 'new_test_user'
        ldap_keyspace = 'customer_ldap'
        InfoEvent(message="Create new user in Scylla").publish()
        self.create_role_in_scylla(node=node, role_name=new_test_user, is_superuser=False,
                                   is_login=True)
        if self.params.get('prepare_saslauthd'):
            self.add_user_in_ldap(username=new_test_user)
        with pytest.raises(Unauthorized, match="has no CREATE permission"):
            with self.db_cluster.cql_connection_patient(node=node, user=new_test_user, password=LDAP_PASSWORD) as session:
                session.execute(
                    f""" CREATE KEYSPACE IF NOT EXISTS {ldap_keyspace} WITH replication = {{'class': 'NetworkTopologyStrategy',
                    'replication_factor': 1}} """)

        InfoEvent(message="Create a new super-user role in Scylla").publish()
        self.create_role_in_scylla(node=node, role_name=superuser_role, is_superuser=True,
                                   is_login=False)
        self.db_cluster.wait_for_schema_agreement()
        InfoEvent(message="Create a new super-user role in Ldap, associated with the new user").publish()

        self.create_role_in_ldap(ldap_role_name=superuser_role, unique_members=[new_test_user, LDAP_USERS[1]])

        self.wait_for_user_roles_update(are_roles_expected=True, username=new_test_user)
        InfoEvent(message="Create keyspace and table where authorized").publish()
        self.wait_verify_user_permissions(username=new_test_user, keyspace=ldap_keyspace)

        # Run a stress while new user permissions are removed
        new_test_user_stress = f"cassandra-stress write no-warmup cl=QUORUM duration=10m -schema 'keyspace={ldap_keyspace} " \
            f" replication(strategy=NetworkTopologyStrategy,replication_factor=3)'" \
            f" -mode cql3 native user={new_test_user} password={LDAP_PASSWORD}" \
            " -rate threads=2 -pop seq=1..1002003 -log interval=5"
        stress_queue.append(self.run_stress_thread(stress_cmd=new_test_user_stress, round_robin=True))
        InfoEvent(message="Let stress of new user run for few minutes before removing permissions").publish()
        time.sleep(120)
        InfoEvent(message="Remove authorization and verify unauthorized user").publish()
        self.modify_ldap_role_delete_member(ldap_role_name=superuser_role, member_name=new_test_user)
        self.wait_for_user_roles_update(are_roles_expected=False, username=new_test_user)
        self.wait_verify_user_no_permissions(username=new_test_user)
        for stress in stress_queue:
            self.verify_stress_thread(stress)

        self.delete_ldap_role(ldap_role_name=superuser_role)
        with pytest.raises(Unauthorized):
            with self.db_cluster.cql_connection_patient(node=node, user=new_test_user, password=LDAP_PASSWORD) as session:
                session.execute(""" DROP KEYSPACE IF EXISTS test_no_permission """)
        with self.db_cluster.cql_connection_patient(node=node, user=LDAP_USERS[0], password=LDAP_PASSWORD) as session:

            session.execute(f""" DROP TABLE IF EXISTS {ldap_keyspace}.info """)
            session.execute(f""" DROP TABLE IF EXISTS {ldap_keyspace}.standard1 """)
            session.execute(f""" DROP KEYSPACE IF EXISTS {ldap_keyspace} """)
            session.execute(""" DROP KEYSPACE IF EXISTS test_no_permission """)
            session.execute(""" DROP KEYSPACE IF EXISTS test_permission_ks """)
            session.execute(f"DROP ROLE IF EXISTS {superuser_role}")
            if self.params.get('prepare_saslauthd'):
                self.delete_ldap_user(ldap_user_name=new_test_user)
                session.execute(f"ALTER ROLE {new_test_user} WITH login=false")
            session.execute(f"DROP ROLE IF EXISTS {new_test_user}")
