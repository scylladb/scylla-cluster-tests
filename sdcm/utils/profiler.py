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
#

import os
import threading
import multiprocessing
import pstats
import cProfile
import shutil
import atexit
import marshal
import time
from typing import List


TIMERS = {
    'perf': time.perf_counter,
    'monotonic': time.monotonic,
    'proc': time.process_time,
}


class ProfileableProcess(multiprocessing.Process):
    """A class derived from multiprocessing.Process that could be profiled via ProfilerFactory

    >>> import time
    >>> from sdcm.utils.profiler import ProfilerFactory, ProfileableProcess
    >>> def thread_body():
    ...     time.sleep(1)
    >>> class MyProfileableProc(ProfileableProcess):
    ...     def execute(self):
    ...         time.sleep(1)
    >>> pf = ProfilerFactory(subcalls=True, target_dir='/tmp/123')
    >>> pf.activate()
    >>> p1 = ProfileableProcess(target=thread_body, name="ProfileableProcess", daemon=True)
    >>> p2 = MyProfileableProc(daemon=True)
    >>> p1.start()
    >>> p2.start()
    >>> p1.join(3)
    >>> p2.join(3)

    """
    _profile = None
    _profile_factory = None

    def init(self):
        ProfileableProcess._bind_profile(self, getattr(self, '_name', 'UnknownProcess'))
        ProfileableProcess.init_run(self)

    @staticmethod
    def init_run(obj):
        original_run_method = obj.run

        def modified_run():
            profile = getattr(obj, '_profile', None)
            if profile:
                profile = profile.fork()
                profile.set_global_main_profile(profile)
                profile.enable()
            original_run_method()
            if profile:
                profile.create_stats()
        setattr(obj, 'run', modified_run)

    def _bind_profile(self, group_name):
        if ProfileableProcess._profile_factory:
            self._profile = ProfileableProcess._profile_factory.get_profile(group_name, is_process=True)

    @staticmethod
    def set_global_profile_factory(factory):
        ProfileableProcess._profile_factory = factory

    @staticmethod
    def get_global_profile_factory():
        return ProfileableProcess._profile_factory

    def start(self):
        ProfileableProcess.init(self)
        REAL_PROCESS_START(self)

    @staticmethod
    def activate():
        multiprocessing.Process._bootstrap_inner = ProfileableProcess._bootstrap
        multiprocessing.Process.start = ProfileableProcess.start

    @staticmethod
    def deactivate():
        multiprocessing.Process._bootstrap_inner = REAL_PROCESS_BOOTSTRAP
        multiprocessing.Process.start = REAL_PROCESS_START


REAL_PROCESS_BOOTSTRAP = multiprocessing.Process._bootstrap
REAL_PROCESS_START = multiprocessing.Process.start


class ProfileableThread(threading.Thread):
    """A class derived from threading.Thread that could be profiled via ProfilerFactory

    >>> import time
    >>> from sdcm.utils.profiler import ProfilerFactory, ProfileableThread
    >>> def thread_body():
    ...     time.sleep(1)
    >>> class MyProfileableThread(ProfileableThread):
    ...     def execute(self):
    ...         time.sleep(1)
    >>> pf = ProfilerFactory(subcalls=True, target_dir='/tmp/123')
    >>> pf.activate()
    >>> p1 = ProfileableThread(target=thread_body, name="ProfileableThread", daemon=True)
    >>> p2 = MyProfileableThread(daemon=True)
    >>> p1.start()
    >>> p2.start()
    >>> p1.join(3)
    >>> p2.join(3)

    """
    _profile = None
    _profile_factory = None

    @staticmethod
    def init(obj):
        if getattr(obj, '_profile', None) is None:
            ProfileableThread.init_profile(obj)

        if obj.run.__name__ != 'modified_run':
            ProfileableThread.init_run(obj)

    @staticmethod
    def init_run(obj):
        original_run_method = obj.run

        def modified_run():
            profile = getattr(obj, '_profile', None)
            if profile:
                profile.enable()
            original_run_method()
            if profile:
                profile.create_stats()
        setattr(obj, 'run', modified_run)

    def _bootstrap_inner(self):
        ProfileableThread.init(self)
        REAL_BOOTSTRAP_INNER(self)

    @staticmethod
    def init_profile(obj):
        if obj.__class__.__name__ == '_DummyThread':
            profile_group_name = multiprocessing.current_process()._name
        else:
            profile_group_name = getattr(obj, '_name', 'UnknownThread')
        ProfileableThread._bind_profile(obj, profile_group_name)

    @staticmethod
    def set_global_profile_factory(factory):
        ProfileableThread._profile_factory = factory

    @staticmethod
    def get_global_profile_factory():
        return ProfileableThread._profile_factory

    def _bind_profile(self, group_name):
        if ProfileableThread._profile_factory:
            self._profile = ProfileableThread._profile_factory.get_profile(group_name, is_process=False)

    @staticmethod
    def activate():
        threading.Thread._bootstrap_inner = ProfileableThread._bootstrap_inner

    @staticmethod
    def deactivate():
        threading.Thread._bootstrap_inner = REAL_BOOTSTRAP_INNER


REAL_BOOTSTRAP_INNER = threading.Thread._bootstrap_inner


def _get_timer_from_str(timer_name):
    return TIMERS.get(timer_name, None)


class StatsHolder:
    """ A class that is designed to hold stats that could be fed to pstats.Stats
    """

    def __init__(self, stats):
        self.stats = self._stats = stats

    def create_stats(self):
        self.stats = self._stats


class ProfileBase(cProfile.Profile):
    _enabled = False
    _type = None
    _args = []
    _kwargs = {}
    _main_profile = None
    _stat_created = False

    def __init__(self, *args, **kwargs):
        new_kwargs = kwargs.copy()
        new_kwargs['timer'] = _get_timer_from_str(new_kwargs.get('timer', None))
        self._group_name = new_kwargs.pop('group_name', None)
        super().__init__(*args, **new_kwargs)
        if args:
            self._args = args
        if kwargs:
            self._kwargs = kwargs

    def enable(self, subcalls=None, builtins=None):
        if not self._enabled:
            subcalls = self._kwargs.get('subcalls', True) if subcalls is None else subcalls
            builtins = self._kwargs.get('builtins', True) if builtins is None else builtins
            super().enable(subcalls, builtins)
            self._enabled = True

    def disable(self):
        if self._enabled:
            self._enabled = False
            super().disable()

    def create_stats(self):
        if self._stat_created:
            return
        self._stat_created = True
        super().create_stats()
        if self._main_profile:
            self._main_profile.send_stats_to_main_process(self.stats)

    def set_main_profile(self, profile):
        self._main_profile = profile

    @staticmethod
    def set_global_main_profile(profile):
        ProfileBase._main_profile = profile


class ThreadProfile(ProfileBase):
    """ A class derived from cProfile.Profile, adopted to ProfilerFactory needs as a profile for threads
    """
    _type = 'thread'

    def get_stats_holder(self):
        if getattr(self, 'stats', None):
            return StatsHolder(self.stats)
        return None


class ProcessProfile(ProfileBase):
    """ A class derived from cProfile.Profile, adopted to ProfilerFactory needs as a profile for processes
    """
    # TBD: ProcessProfile won't get it's stats unless process is stopped to address that it is necessary to have
    # thread inside that proccess that will disable, create and dump stats by the command from master profile.
    _type = 'process'

    def __init__(self, *args, queue=None, **kwargs):
        super().__init__(*args, **kwargs)
        if queue:
            self._master = False
            self._queue = queue
        else:
            self._master = True
            self._queue = multiprocessing.Queue()

    def fork(self) -> 'ProcessProfile':
        """
        Make a slave fork of a main profile and passed queue down to it
        To be used inside of process context, so that slave profile could send profile stats
          via queue to the main profile
        """
        return self.__class__(*self._args, queue=self._queue, **self._kwargs)

    def send_stats_to_main_process(self, stats):
        self._queue.put(stats)

    def get_stats_holders(self) -> List[StatsHolder]:
        """
        Get slave's stats via queue, make list of StatsHolder of them and return it.
        """
        output = []
        while not self._queue.empty():
            stats = self._queue.get(block=False)
            if stats:
                output.append(StatsHolder(stats))
        return output


class ProfilerFactory:
    """A class that manages profiling on different levels - main thread, Threads, Processes.

    >>> import time
    >>> from sdcm.utils.profiler import ProfilerFactory, ProfileableThread
    ... def thread_body():
    ...     time.sleep(1)
    >>> class MyProfileableProc(ProfileableProcess):
    ...     def execute(self):
    ...         time.sleep(1)
    >>> pf = ProfilerFactory(subcalls=True, target_dir='/tmp/123')
    >>> pf.activate()
    >>> p1 = ProfileableProcess(target=thread_body, name="ProfileableProcess", daemon=True)
    >>> p2 = MyProfileableProc(daemon=True)
    >>> p1.start()
    >>> p2.start()
    >>> p1.join(3)
    >>> p2.join(3)

    After running the code you can see textual stats via:
        cat /tmp/123/stats.txt
    Or you can invoke snakeviz to represent stats:
        snakeviz /tmp/123/stats.bin
    Or you can see call graph:
        gprof2dot -f pstats /tmp/123/stats.bin | dot -Tpng -o /tmp/123/stats.callgraph.png
    Or you can need summarized call graph:
        tuna /tmp/123/stats.bin
    """

    def __init__(self, target_dir: str, subcalls=True, builtins=True, timer: str = 'proc'):
        self._profiles = {}
        self._subcalls = subcalls
        self._builtins = builtins
        self._target_dir = target_dir
        self._timer = timer

    def activate(self, main=True, processes=True, threads=True, autodump=True):
        if processes:
            ProfileableProcess.set_global_profile_factory(self)
            ProfileableProcess.activate()
        if threads:
            ProfileableThread.set_global_profile_factory(self)
            ProfileableThread.activate()
        if main:
            self.get_profile('main').enable()
        if autodump:
            atexit.register(self.stop_and_dump)

    @staticmethod
    def deactivate():
        ProfileableThread.deactivate()
        ProfileableProcess.deactivate()

    def get_profile(self, group_name, is_process=False):
        if is_process:
            profile = ProcessProfile(timer=self._timer, subcalls=self._subcalls,
                                     builtins=self._builtins, group_name=group_name)
        else:
            profile = ThreadProfile(timer=self._timer, subcalls=self._subcalls,
                                    builtins=self._builtins, group_name=group_name)
        self._store_profile(group_name, profile)
        return profile

    def _store_profile(self, group_name, profile):
        profiles = self._profiles.get(group_name, None)
        if profiles is None:
            self._profiles[group_name] = profiles = []
        profiles.append(profile)

    def stop_and_dump(self):
        self.deactivate()
        self.stop_all()
        self.dump_stats()

    def stop_all(self):
        for profiles in self._profiles.values():
            for profile in profiles:
                profile.disable()
                profile.create_stats()

    @staticmethod
    def _dump_stats(*stat_holders, dst=None, binary=False, sort=None):
        stat = pstats.Stats(*stat_holders, stream=dst)
        if sort:
            stat.sort_stats(sort)
        if binary:
            marshal.dump(stat.stats, dst)
        else:
            stat.print_stats()

    def _dump_text_stats(self, *stat_holders, dst=None):
        dst.write("=============== ncalls ===============\n")
        self._dump_stats(*stat_holders, dst=dst, binary=False, sort='ncalls')
        dst.write("=============== tottime ===============\n")
        self._dump_stats(*stat_holders, dst=dst, binary=False, sort='tottime')
        dst.write("=============== pcalls ===============\n")
        self._dump_stats(*stat_holders, dst=dst, binary=False, sort='pcalls')
        dst.write("=============== cumtime ===============\n")
        self._dump_stats(*stat_holders, dst=dst, binary=False, sort='cumtime')

    def dump_stats(self):
        stats_holders = {}
        for group_name, profiles in self._profiles.items():
            stats_holders[group_name] = group_bucket = []
            for profile in profiles:
                if isinstance(profile, ProcessProfile):
                    group_bucket.extend(profile.get_stats_holders())
                if isinstance(profile, ThreadProfile):
                    stat_holder = profile.get_stats_holder()
                    if stat_holder:
                        group_bucket.append(stat_holder)
        total_stats = []
        for group_name, stat_holders in stats_holders.items():
            if not stat_holders:
                continue
            total_stats.extend(stat_holders)
            group_dir = os.path.join(self._target_dir, group_name)
            if os.path.exists(group_dir):
                shutil.rmtree(group_dir, ignore_errors=True)
            os.makedirs(group_dir, exist_ok=True)
            with open(os.path.join(group_dir, 'stats.txt'), 'w', encoding="utf-8") as stat_file:
                self._dump_text_stats(*stat_holders, dst=stat_file)
            with open(os.path.join(group_dir, 'stats.bin'), 'wb') as stat_file:
                self._dump_stats(*stat_holders, dst=stat_file, binary=True)
        with open(os.path.join(self._target_dir, 'stats.txt'), 'w', encoding="utf-8") as stat_file:
            self._dump_text_stats(*total_stats, dst=stat_file)
        with open(os.path.join(self._target_dir, 'stats.bin'), 'wb') as stat_file:
            self._dump_stats(*total_stats, dst=stat_file, binary=True)
