import StringIO
import glob
import logging
import os
import re
import shlex
import shutil
import signal
import socket
import subprocess
import tempfile
import threading
import time

from avocado.utils import astring
from avocado.utils import path
from avocado.utils import process

ENABLE_MASTER_SSH = True
LOG = process.log
STDOUT_LOG = process.stdout_log
STDERR_LOG = process.stderr_log


class SSHTimeout(Exception):

    """
    The SSH connection reported a connection timeout.
    """
    pass


class SSHPermissionDeniedError(Exception):

    """
    The SSH connection reported a permission denied error.
    """
    pass


class OutputCheckError(Exception):

    """
    Remote command output check failed.
    """
    pass


class SSHSubProcess(process.SubProcess):

    def __init__(self, cmd, verbose=True, allow_output_check='all',
                 shell=False, env=None, extra_text=None):
        super(SSHSubProcess, self).__init__(cmd=cmd, verbose=verbose,
                                            allow_output_check=allow_output_check,
                                            shell=shell, env=env)
        self.extra_text = extra_text

    def _init_subprocess(self):
        if self._popen is None:
            if self.verbose:
                cmd_msg = ''
                if self.extra_text:
                    cmd_msg += '[%s] ' % self.extra_text
                cmd_msg += "Running '%s'" % self.cmd
                LOG.info(cmd_msg)
            if self.shell is False:
                cmd = shlex.split(self.cmd)
            else:
                cmd = self.cmd
            try:
                self._popen = subprocess.Popen(cmd,
                                               stdout=subprocess.PIPE,
                                               stderr=subprocess.PIPE,
                                               shell=self.shell,
                                               env=self.env)
            except OSError, details:
                if details.errno == 2:
                    exc = OSError("File '%s' not found" % self.cmd.split()[0])
                    exc.errno = 2
                    raise exc
                else:
                    raise

            self.start_time = time.time()
            self.stdout_file = StringIO.StringIO()
            self.stderr_file = StringIO.StringIO()
            self.stdout_lock = threading.Lock()
            self.stdout_thread = threading.Thread(target=self._fd_drainer,
                                                  name="%s-stdout" % self.cmd,
                                                  args=[self._popen.stdout])
            self.stdout_thread.daemon = True
            self.stderr_lock = threading.Lock()
            self.stderr_thread = threading.Thread(target=self._fd_drainer,
                                                  name="%s-stderr" % self.cmd,
                                                  args=[self._popen.stderr])
            self.stderr_thread.daemon = True
            self.stdout_thread.start()
            self.stderr_thread.start()

            def signal_handler(signum, frame):
                self.result.interrupted = True
                self.wait()

            try:
                signal.signal(signal.SIGINT, signal_handler)
            except ValueError:
                pass

    def _fd_drainer(self, input_pipe):
        stream_prefix = "%s"
        prefix = ''
        if self.extra_text:
            prefix = '[%s] ' % self.extra_text
        if input_pipe == self._popen.stdout:
            prefix += '[stdout] %s'
            if self.allow_output_check in ['none', 'stderr']:
                stream_logger = None
            else:
                stream_logger = STDOUT_LOG
            output_file = self.stdout_file
            lock = self.stdout_lock
        elif input_pipe == self._popen.stderr:
            prefix += '[stderr] %s'
            if self.allow_output_check in ['none', 'stdout']:
                stream_logger = None
            else:
                stream_logger = STDERR_LOG
            output_file = self.stderr_file
            lock = self.stderr_lock

        fileno = input_pipe.fileno()

        bfr = ''
        while True:
            tmp = os.read(fileno, 1024)
            if tmp == '':
                if self.verbose and bfr:
                    for line in bfr.splitlines():
                        LOG.debug(prefix, line)
                        if stream_logger is not None:
                            stream_logger.debug(stream_prefix, line)
                break
            lock.acquire()
            try:
                output_file.write(tmp)
                if self.verbose:
                    bfr += tmp
                    if tmp.endswith('\n'):
                        for line in bfr.splitlines():
                            LOG.debug(prefix, line)
                            if stream_logger is not None:
                                stream_logger.debug(stream_prefix, line)
                        bfr = ''
            finally:
                lock.release()


def ssh_run(cmd, timeout=None, verbose=True, ignore_status=False,
            allow_output_check='all', shell=False, env=None,
            extra_text=None):
    sp = SSHSubProcess(cmd=cmd, verbose=verbose,
                       allow_output_check=allow_output_check, shell=shell,
                       env=env, extra_text=extra_text)
    cmd_result = sp.run(timeout=timeout)
    fail_condition = cmd_result.exit_status != 0 or cmd_result.interrupted
    if fail_condition and not ignore_status:
        raise process.CmdError(cmd, sp.result)
    return cmd_result


def _scp_remote_escape(filename):
    """
    Escape special chars for SCP use.

    Bis-quoting has to be used with scp for remote files, "bis-quoting"
    as in quoting x 2. SCP does not support a newline in the filename.

    :param filename: the filename string to escape.

    :returns: The escaped filename string. The required englobing double
        quotes are NOT added and so should be added at some point by
        the caller.
    """
    escape_chars = r' !"$&' "'" r'()*,:;<=>?[\]^`{|}'

    new_name = []
    for char in filename:
        if char in escape_chars:
            new_name.append("\\%s" % (char,))
        else:
            new_name.append(char)

    return astring.shell_escape("".join(new_name))


def _make_ssh_command(user="root", port=22, opts='', hosts_file='/dev/null',
                      key_file=None, connect_timeout=300, alive_interval=300):
    assert isinstance(connect_timeout, (int, long))
    base_command = path.find_command('ssh')
    base_command += (" -a -x %s -o StrictHostKeyChecking=no "
                     "-o UserKnownHostsFile=%s -o BatchMode=yes "
                     "-o ConnectTimeout=%d -o ServerAliveInterval=%d "
                     "-l %s -p %d")
    if key_file is not None:
        base_command += ' -i %s' % key_file
    assert connect_timeout > 0  # can't disable the timeout
    return base_command % (opts, hosts_file, connect_timeout,
                           alive_interval, user, port)


class BaseRemote(object):

    def __init__(self, hostname, user="root", port=22, password="",
                 key_file=None):
        self.env = {}
        self.hostname = hostname
        self.ip = socket.getaddrinfo(self.hostname, None)[0][4][0]
        self.user = user
        self.port = port
        self.password = password
        self.key_file = key_file
        self._use_rsync = None
        self.known_hosts_file = tempfile.mkstemp()[1]
        self.master_ssh_job = None
        self.master_ssh_tempdir = None
        self.master_ssh_option = ''

    def __str__(self):
        return 'Remote [{}@{}]'.format(self.user, self.hostname)

    def use_rsync(self):
        if self._use_rsync is not None:
            return self._use_rsync

        # Check if rsync is available on the remote host. If it's not,
        # don't try to use it for any future file transfers.
        self._use_rsync = self._check_rsync()
        if not self._use_rsync:
            logging.warn("rsync not available on remote host %s -- disabled",
                         self.hostname)
        return self._use_rsync

    def _check_rsync(self):
        """
        Check if rsync is available on the remote host.
        """
        try:
            self.run("rsync --version", verbose=False)
        except process.CmdError:
            return False
        return True

    def _encode_remote_paths(self, paths, escape=True):
        """
        Given a list of file paths, encodes it as a single remote path, in
        the style used by rsync and scp.
        """
        if escape:
            paths = [_scp_remote_escape(path) for path in paths]
        return '%s@%s:"%s"' % (self.user, self.hostname, " ".join(paths))

    def _make_rsync_cmd(self, src, dst, delete_dst, preserve_symlinks):
        """
        Given a list of source paths and a destination path, produces the
        appropriate rsync command for copying them. Remote paths must be
        pre-encoded.
        """
        ssh_cmd = _make_ssh_command(user=self.user, port=self.port,
                                    opts=self.master_ssh_option,
                                    hosts_file=self.known_hosts_file,
                                    key_file=self.key_file)
        if delete_dst:
            delete_flag = "--delete"
        else:
            delete_flag = ""
        if preserve_symlinks:
            symlink_flag = ""
        else:
            symlink_flag = "-L"
        command = "rsync %s %s --timeout=1800 --rsh='%s' -az %s %s"
        return command % (symlink_flag, delete_flag, ssh_cmd,
                          " ".join(src), dst)

    def _make_ssh_cmd(self, cmd):
        """
        Create a base ssh command string for the host which can be used
        to run commands directly on the machine
        """
        base_cmd = _make_ssh_command(user=self.user, port=self.port,
                                     key_file=self.key_file,
                                     opts=self.master_ssh_option,
                                     hosts_file=self.known_hosts_file)

        return '%s %s "%s"' % (base_cmd, self.hostname,
                               astring.shell_escape(cmd))

    def _make_scp_cmd(self, src, dst):
        """
        Given a list of source paths and a destination path, produces the
        appropriate scp command for encoding it. Remote paths must be
        pre-encoded.
        """
        command = ("scp -rq %s -o StrictHostKeyChecking=no "
                   "-o UserKnownHostsFile=%s -P %d %s '%s'")
        return command % (self.master_ssh_option, self.known_hosts_file,
                          self.port, " ".join(src), dst)

    def _make_rsync_compatible_globs(self, pth, is_local):
        """
        Given an rsync-style path (pth), returns a list of globbed paths.

        Those will hopefully provide equivalent behaviour for scp. Does not
        support the full range of rsync pattern matching behaviour, only that
        exposed in the get/send_file interface (trailing slashes).

        :param pth: rsync-style path.
        :param is_local: Whether the paths should be interpreted as local or
            remote paths.
        """

        # non-trailing slash paths should just work
        if len(pth) == 0 or pth[-1] != "/":
            return [pth]

        # make a function to test if a pattern matches any files
        if is_local:
            def glob_matches_files(path, pattern):
                return len(glob.glob(path + pattern)) > 0
        else:
            def glob_matches_files(path, pattern):
                match_cmd = "ls \"%s\"%s" % (astring.shell_escape(path), pattern)
                result = self.run(match_cmd, ignore_status=True)
                return result.exit_status == 0

        # take a set of globs that cover all files, and see which are needed
        patterns = ["*", ".[!.]*"]
        patterns = [p for p in patterns if glob_matches_files(pth, p)]

        # convert them into a set of paths suitable for the commandline
        if is_local:
            return ["\"%s\"%s" % (astring.shell_escape(pth), pattern)
                    for pattern in patterns]
        else:
            return [_scp_remote_escape(pth) + pattern
                    for pattern in patterns]

    def _make_rsync_compatible_source(self, source, is_local):
        """
        Make an rsync compatible source string.

        Applies the same logic as _make_rsync_compatible_globs, but
        applies it to an entire list of sources, producing a new list of
        sources, properly quoted.
        """
        return sum((self._make_rsync_compatible_globs(path, is_local)
                    for path in source), [])

    def _set_umask_perms(self, dest):
        """
        Set permissions on all files and directories.

        Given a destination file/dir (recursively) set the permissions on
        all the files and directories to the max allowed by running umask.
        """

        # now this looks strange but I haven't found a way in Python to _just_
        # get the umask, apparently the only option is to try to set it
        umask = os.umask(0)
        os.umask(umask)

        max_privs = 0777 & ~umask

        def set_file_privs(filename):
            file_stat = os.stat(filename)

            file_privs = max_privs
            # if the original file permissions do not have at least one
            # executable bit then do not set it anywhere
            if not file_stat.st_mode & 0111:
                file_privs &= ~0111

            os.chmod(filename, file_privs)

        # try a bottom-up walk so changes on directory permissions won't cut
        # our access to the files/directories inside it
        for root, dirs, files in os.walk(dest, topdown=False):
            # when setting the privileges we emulate the chmod "X" behaviour
            # that sets to execute only if it is a directory or any of the
            # owner/group/other already has execute right
            for dirname in dirs:
                os.chmod(os.path.join(root, dirname), max_privs)

            for filename in files:
                set_file_privs(os.path.join(root, filename))

        # now set privs for the dest itself
        if os.path.isdir(dest):
            os.chmod(dest, max_privs)
        else:
            set_file_privs(dest)

    def ssh_command(self, connect_timeout=300, options='', alive_interval=300):
        options = "%s %s" % (options, self.master_ssh_option)
        base_cmd = _make_ssh_command(user=self.user, port=self.port,
                                     key_file=self.key_file,
                                     opts=options,
                                     hosts_file=self.known_hosts_file,
                                     connect_timeout=connect_timeout,
                                     alive_interval=alive_interval)
        return "%s %s" % (base_cmd, self.hostname)

    def run(self, command, timeout=3600, ignore_status=False,
            connect_timeout=300, options='', verbose=True, args=None):
        raise NotImplementedError("Subclasses must implement "
                                  "the method 'run' ")

    def receive_files(self, src, dst, delete_dst=False,
                      preserve_perm=True, preserve_symlinks=False,
                      verbose=False):
        """
        Copy files from the remote host to a local path.

        If both machines support rsync, that command will be used.

        If not, an scp command will be assembled. Directories will be
        copied recursively. If a src component is a directory with a
        trailing slash, the content of the directory will be copied,
        otherwise, the directory itself and its content will be copied. This
        behavior is similar to that of the program 'rsync'.

        :param src: Either
            1) a single file or directory, as a string
            2) a list of one or more (possibly mixed) files or directories
        :param dst: A file or a directory (if src contains a
            directory or more than one element, you must supply a
            directory dst).
        :param delete_dst: If this is true, the command will also clear
            out any old files at dest that are not in the src
        :param preserve_perm: Tells get_file() to try to preserve the sources
            permissions on files and dirs.
        :param preserve_symlinks: Try to preserve symlinks instead of
            transforming them into files/dirs on copy.
        :param verbose: Log commands being used and their outputs.

        :raises: process.CmdError if the remote copy command failed.
        """
        print("{}: Receive files (src) {} -> (dst) {}".format(str(self),
                                                              src,
                                                              dst))
        # Start a master SSH connection if necessary.
        self.start_master_ssh()

        if isinstance(src, basestring):
            src = [src]
        dst = os.path.abspath(dst)

        # If rsync is disabled or fails, try scp.
        try_scp = True
        if self.use_rsync():
            try:
                remote_source = self._encode_remote_paths(src)
                local_dest = astring.shell_escape(dst)
                rsync = self._make_rsync_cmd([remote_source], local_dest,
                                             delete_dst, preserve_symlinks)
                ssh_run(rsync, shell=True, extra_text=self.hostname,
                        verbose=verbose)
                try_scp = False
            except process.CmdError, e:
                logging.warn("trying scp, rsync failed: %s" % e)

        if try_scp:
            # scp has no equivalent to --delete, just drop the entire dest dir
            if delete_dst and os.path.isdir(dst):
                shutil.rmtree(dst)
                os.mkdir(dst)

            remote_source = self._make_rsync_compatible_source(src, False)
            if remote_source:
                # _make_rsync_compatible_source() already did the escaping
                remote_source = self._encode_remote_paths(remote_source,
                                                          escape=False)
                local_dest = astring.shell_escape(dst)
                scp = self._make_scp_cmd([remote_source], local_dest)
                ssh_run(scp, shell=True, extra_text=self.hostname,
                        verbose=verbose)

        if not preserve_perm:
            # we have no way to tell scp to not try to preserve the
            # permissions so set them after copy instead.
            # for rsync we could use "--no-p --chmod=ugo=rwX" but those
            # options are only in very recent rsync versions
            self._set_umask_perms(dst)

    def send_files(self, src, dst, delete_dst=False,
                   preserve_symlinks=False, verbose=False):
        """
        Copy files from a local path to the remote host.

        If both machines support rsync, that command will be used.

        If not, an scp command will be assembled. Directories will be
        copied recursively. If a src component is a directory with a
        trailing slash, the content of the directory will be copied,
        otherwise, the directory itself and its content will be copied. This
        behavior is similar to that of the program 'rsync'.

        :param src: Either
            1) a single file or directory, as a string
            2) a list of one or more (possibly mixed) files or directories
        :param dst: A file or a directory (if src contains a
            directory or more than one element, you must supply a
            directory dst).
        :param delete_dst: If this is true, the command will also clear
            out any old files at dest that are not in the src
        :param preserve_symlinks: Try to preserve symlinks instead of
            transforming them into files/dirs on copy.
        :param verbose: Log commands being used and their outputs.

        :raises: process.CmdError if the remote copy command failed
        """
        print("{}: Send files (src) {} -> (dst) {}".format(str(self),
                                                           src,
                                                           dst))
        # Start a master SSH connection if necessary.
        self.start_master_ssh()

        if isinstance(src, basestring):
            source_is_dir = os.path.isdir(src)
            src = [src]
        remote_dest = self._encode_remote_paths([dst])

        # If rsync is disabled or fails, try scp.
        try_scp = True
        if self.use_rsync():
            try:
                local_sources = [astring.shell_escape(path) for path in src]
                rsync = self._make_rsync_cmd(local_sources, remote_dest,
                                             delete_dst, preserve_symlinks)
                ssh_run(rsync, shell=True, extra_text=self.hostname,
                        verbose=verbose)
                try_scp = False
            except process.CmdError, details:
                logging.warn("trying scp, rsync failed: %s" % details)

        if try_scp:
            # scp has no equivalent to --delete, just drop the entire dest dir
            if delete_dst:
                dest_exists = False
                try:
                    self.run("test -x %s" % dst, verbose=verbose)
                    dest_exists = True
                except process.CmdError:
                    pass

                dest_is_dir = False
                if dest_exists:
                    try:
                        self.run("test -d %s" % dst, verbose=verbose)
                        dest_is_dir = True
                    except process.CmdError:
                        pass

                # If there is a list of more than one path, dst *has*
                # to be a dir. If there's a single path being transferred and
                # it is a dir, the dst also has to be a dir. Therefore
                # it has to be created on the remote machine in case it doesn't
                # exist, otherwise we will have an scp failure.
                if len(src) > 1 or source_is_dir:
                    dest_is_dir = True

                if dest_exists and dest_is_dir:
                    cmd = "rm -rf %s && mkdir %s" % (dst, dst)
                    self.run(cmd, verbose=verbose)

                elif not dest_exists and dest_is_dir:
                    cmd = "mkdir %s" % dst
                    self.run(cmd, verbose=verbose)

            local_sources = self._make_rsync_compatible_source(src, True)
            if local_sources:
                scp = self._make_scp_cmd(local_sources, remote_dest)
                ssh_run(scp, shell=True, extra_text=self.hostname,
                        verbose=verbose)

    def _ssh_ping(self, timeout=30):
        try:
            self.run("true", timeout=timeout, connect_timeout=timeout,
                     verbose=False)
        except SSHTimeout:
            msg = "Host (ssh) verify timed out (timeout = %d)" % timeout
            raise SSHTimeout(msg)
        except SSHPermissionDeniedError:
            raise

    def is_up(self):
        """
        Check if the remote host is up.

        :return: True if the remote host is up, False otherwise
        """
        try:
            self._ssh_ping()
        except process.CmdError:
            return False
        else:
            return True

    def close(self):
        self._cleanup_master_ssh()
        os.remove(self.known_hosts_file)

    def __del__(self):
        self.close()

    def _cleanup_master_ssh(self):
        """
        Release all resources used by the master SSH connection.

        Such resources are processes and temporary files.
        """
        # If a master SSH connection is running, kill it.
        if self.master_ssh_job is not None:
            try:
                self.master_ssh_job.kill()
            except OSError:
                pass
            finally:
                self.master_ssh_job = None

        # Remove the temporary directory for the master SSH socket.
        if self.master_ssh_tempdir is not None:
            shutil.rmtree(self.master_ssh_tempdir, ignore_errors=True)
            self.master_ssh_tempdir = None
            self.master_ssh_option = ''

    def start_master_ssh(self):
        """
        Start a master SSH connection.

        If master SSH support is enabled and a master SSH connection is not
        active already, start a new one in the background. Also, cleanup any
        zombie master SSH connections (e.g., dead due to reboot).
        """
        if not ENABLE_MASTER_SSH:
            return

        # If a previously started master SSH connection is not running
        # anymore, it needs to be cleaned up and then restarted.
        if self.master_ssh_job is not None:
            if self.master_ssh_job.poll() is not None:
                self._cleanup_master_ssh()

        # Start a new master SSH connection.
        if self.master_ssh_job is None:
            # Create a shared socket in a temp location.
            self.master_ssh_tempdir = tempfile.mkdtemp(prefix='ssh-master')
            self.master_ssh_option = ("-o ControlPath=%s/socket" %
                                      self.master_ssh_tempdir)

            # Start the master SSH connection in the background.
            master_cmd = self.ssh_command(options="-N -o ControlMaster=yes")
            self.master_ssh_job = SSHSubProcess(cmd=master_cmd,
                                                verbose=False,
                                                extra_text=self.hostname)
            self.master_ssh_job.start()


class Remote(BaseRemote):

    def __init__(self, hostname, user="root", port=22, password="",
                 key_file=None):
        super(Remote, self).__init__(hostname=hostname, user=user,
                                     port=port, password=password,
                                     key_file=key_file)
        self.run_quiet = self.run

    def ssh_command(self, connect_timeout=300, options='', alive_interval=300):
        options = "%s %s" % (options, self.master_ssh_option)
        base_cmd = _make_ssh_command(user=self.user, port=self.port,
                                     key_file=self.key_file,
                                     opts=options,
                                     hosts_file=self.known_hosts_file,
                                     connect_timeout=connect_timeout,
                                     alive_interval=alive_interval)
        return "%s %s" % (base_cmd, self.hostname)

    def _run(self, cmd, timeout, verbose, ignore_status, connect_timeout,
             env, options, args):
        ssh_cmd = self.ssh_command(connect_timeout, options)
        if not env.strip():
            env = ""
        else:
            env = "export %s;" % env
        for arg in args:
            cmd += ' "%s"' % astring.shell_escape(arg)
        if env:
            full_cmd = '%s "%s %s"' % (ssh_cmd, env, astring.shell_escape(cmd))
        else:
            full_cmd = '%s "%s"' % (ssh_cmd, astring.shell_escape(cmd))
        result = ssh_run(full_cmd, verbose=verbose,
                         ignore_status=ignore_status, timeout=timeout,
                         extra_text=self.hostname, shell=True)

        # The error messages will show up in band (indistinguishable
        # from stuff sent through the SSH connection), so we have the
        # remote computer echo the message "Connected." before running
        # any cmd.  Since the following 2 errors have to do with
        # connecting, it's safe to do these checks.
        if result.exit_status == 255:
            if re.search(r'^ssh: connect to host .* port .*: '
                         r'Connection timed out\r$', result.stderr):
                raise SSHTimeout("ssh timed out", result)
            if "Permission denied." in result.stderr:
                msg = "SSH permission denied"
                raise SSHPermissionDeniedError(msg, result)
        if not ignore_status and result.exit_status > 0:
            raise process.CmdError(command=full_cmd, result=result)
        return result

    def run(self, cmd, timeout=3600, ignore_status=False,
            connect_timeout=300, options='', verbose=True,
            args=None):
        if args is None:
            args = ()
        if verbose:
            logging.debug("[%s] Running (ssh) '%s'", self.hostname, cmd)

        # Start a master SSH connection if necessary.
        self.start_master_ssh()

        env = " ".join("=".join(pair) for pair in self.env.iteritems())
        return self._run(cmd=cmd, timeout=timeout, verbose=verbose,
                         ignore_status=ignore_status,
                         connect_timeout=connect_timeout,
                         env=env, options=options, args=args)

    def run_output_check(self, cmd, timeout=30, ignore_status=False,
                         stdout_ok_regexp=None, stdout_err_regexp=None,
                         stderr_ok_regexp=None, stderr_err_regexp=None,
                         connect_timeout=300):
        """
        Run a cmd on the remote host, check output to determine success.

        :param cmd: The cmd line string.
        :param timeout: Time limit in seconds before attempting to kill the
            running process. The run() function will take a few seconds
            longer than 'timeout' to complete if it has to kill the process.
        :param ignore_status: Do not raise an exception, no matter
            what the exit code of the cmd is.
        :param stdout_ok_regexp: Regular expression that should be in stdout
            if the cmd was successul.
        :param stdout_err_regexp: Regular expression that should be in stdout
            if the cmd failed.
        :param stderr_ok_regexp: regexp that should be in stderr if the
            cmd was successul.
        :param stderr_err_regexp: Regexp that should be in stderr if the
            cmd failed.
        :param connect_timeout: Connection timeout that will be passed to run.

        :raises: OutputCheckError under the following conditions:
            - The exit code of the cmd execution was not 0.
            - If stderr_err_regexp is found in stderr,
            - If stdout_err_regexp is found in stdout,
            - If stderr_ok_regexp is not found in stderr.
            - If stdout_ok_regexp is not found in stdout,
        """

        # We ignore the status, because we will handle it at the end.
        result = self.run(cmd, timeout, ignore_status=True,
                          connect_timeout=connect_timeout)

        # Look for the patterns, in order
        for (regexp, stream) in ((stderr_err_regexp, result.stderr),
                                 (stdout_err_regexp, result.stdout)):
            if regexp and stream:
                err_re = re.compile(regexp)
                if err_re.search(stream):
                    e_msg = ('%s failed, found error pattern: "%s"' %
                             (cmd, regexp))
                    raise OutputCheckError(e_msg, result)

        for (regexp, stream) in ((stderr_ok_regexp, result.stderr),
                                 (stdout_ok_regexp, result.stdout)):
            if regexp and stream:
                ok_re = re.compile(regexp)
                if ok_re.search(stream):
                    if ok_re.search(stream):
                        return

        if not ignore_status and result.exit_status > 0:
            raise process.CmdError(command=result.command, result=result)
