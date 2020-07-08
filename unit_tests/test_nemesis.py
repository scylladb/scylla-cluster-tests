from collections import namedtuple
import sdcm.utils.cloud_monitor  # pylint: disable=unused-import # import only to avoid cyclic dependency
from sdcm.nemesis import Nemesis, ChaosMonkey


PARAMS = dict(nemesis_interval=1, nemesis_filter_seeds=False)
Cluster = namedtuple("Cluster", ['params'])
FakeTester = namedtuple("FakeTester", ['params', 'loaders', 'monitors', 'db_cluster'],
                        defaults=[PARAMS, {}, {}, Cluster(params=PARAMS)])


class AddRemoveDCMonkey(Nemesis):
    @Nemesis.add_disrupt_method
    def disrupt_add_remove_dc(self):  # pylint: disable=no-self-use
        return 'Worked'

    def disrupt(self):
        self.disrupt_add_remove_dc()


def test_list_nemesis_of_added_disrupt_methods():
    nemesis = ChaosMonkey(FakeTester(), None)
    assert 'disrupt_add_remove_dc' in nemesis.get_list_of_disrupt_methods_for_nemesis_subclasses(disruptive=False)
    assert nemesis.call_random_disrupt_method(disrupt_methods=['disrupt_add_remove_dc']) is None
