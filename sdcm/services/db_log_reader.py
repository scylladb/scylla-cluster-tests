import re
import queue

from sdcm.services.base import NodeThreadService


class DBLogReaderThread(NodeThreadService):
    stop_priority = 90
    _interval = 0.1
    _exclude_system_log_from_being_logged = [
        ' !INFO    | sshd[',
        ' !INFO    | systemd:',
        ' !INFO    | systemd-logind:',
        ' !INFO    | sudo:',
        ' !INFO    | sshd[',
        ' !INFO    | dhclient['
    ]
    _backtrace_regexp = re.compile(r'(?P<other_bt>/lib.*?\+0x[0-f]*\n)|(?P<scylla_bt>0x[0-f]*\n)', re.IGNORECASE)
    _last_log_position = 0

    def __init__(
            self,
            node,
            log_file_records: bool = False,
            backtrace_decoding_queue: queue.Queue = None,
            event_rules: list = None):
        self._log_file_records = log_file_records
        self._last_line_no = 0
        self._backtrace_decoding_queue = backtrace_decoding_queue
        self._system_log_errors_index = []
        self._event_rules = event_rules
        super().__init__(node)

    def _service_body(self):
        """
        Keep reporting new events from db log, every 30 seconds.
        """
        # pylint: disable=too-many-branches,too-many-locals,too-many-statements

        backtraces = []
        index = 0
        # prepare/compile all regexes

        start_search_from_byte = self._last_log_position
        last_line_no = self._last_line_no

        with open(self._node.system_log, 'r') as db_file:
            if start_search_from_byte:
                db_file.seek(start_search_from_byte)
            for index, line in enumerate(db_file, start=last_line_no):
                if self._log_file_records:
                    line = line.strip()
                    exclude = False
                    for pattern in self._exclude_system_log_from_being_logged:
                        if pattern in line:
                            exclude = True
                            break
                    if not exclude:
                        self._log.debug(line)
                match = self._backtrace_regexp.search(line)
                if match and backtraces:
                    data = match.groupdict()
                    if data['other_bt']:
                        backtraces[-1]['backtrace'] += [data['other_bt'].strip()]
                    if data['scylla_bt']:
                        backtraces[-1]['backtrace'] += [data['scylla_bt'].strip()]

                if index not in self._system_log_errors_index:
                    # for each line use all regexes to match, and if found send an event
                    for pattern, event in self._event_rules:
                        match = pattern.search(line)
                        if match:
                            self._system_log_errors_index.append(index)
                            cloned_event = event.clone_with_info(node=self, line_number=index, line=line)
                            backtraces.append(dict(event=cloned_event, backtrace=[]))
                            break  # Stop iterating patterns to avoid creating two events for one line of the log

            self._last_line_no = index if index else last_line_no
            self._last_log_position = db_file.tell() + 1

        traces_count = 0
        for backtrace in backtraces:
            backtrace['event'].add_backtrace_info(raw_backtrace="\n".join(backtrace['backtrace']))
            if backtrace['event'].type == 'BACKTRACE':
                traces_count += 1

        # filter function to attach the backtrace to the correct error and not to the back traces
        # if the error is within 10 lines and the last isn't backtrace type, the backtrace would be appended to the previous error
        def filter_backtraces(backtrace):
            last_error = filter_backtraces.last_error
            try:
                if (last_error and
                        backtrace['event'].line_number <= filter_backtraces.last_error.line_number + 20
                        and not filter_backtraces.last_error.type == 'BACKTRACE' and backtrace[
                            'event'].type == 'BACKTRACE'):
                    last_error.add_backtrace_info(raw_backtrace="\n".join(backtrace['backtrace']))
                    return False
                return True
            finally:
                filter_backtraces.last_error = backtrace['event']

        # support interlaced reactor stalled
        for _ in range(traces_count):
            filter_backtraces.last_error = None
            backtraces = list(filter(filter_backtraces, backtraces))

        for backtrace in backtraces:
            if self._backtrace_decoding_queue:
                if backtrace['event'].raw_backtrace:
                    scylla_debug_info = self._node.get_scylla_debuginfo_file()
                    self._log.debug('Debug info file %s', scylla_debug_info)
                    self._backtrace_decoding_queue.put(
                        {"node": self, "debug_file": scylla_debug_info, "event": backtrace['event']})
            else:
                backtrace['event'].publish()
