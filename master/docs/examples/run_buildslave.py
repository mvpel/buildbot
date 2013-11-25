#!/usr/bin/env python
#
# This file is part of Buildbot.  Buildbot is free software: you can
# redistribute it and/or modify it under the terms of the GNU General Public
# License as published by the Free Software Foundation, version 2.
#
# This program is distributed in the hope that it will be useful, but WITHOUT
# ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
# FOR A PARTICULAR PURPOSE.  See the GNU General Public License for more
# details.
#
# You should have received a copy of the GNU General Public License along with
# this program; if not, write to the Free Software Foundation, Inc., 51
# Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA.
#
# Copyright Buildbot Team Members
"""
This script creates and starts a new buildslave based on the options provided,
to support latent buildslaves dispatched by the HTCondor or Grid Engine
schedulers. The slave is created in the scratch directory designated by TMPDIR
in the execution environment. The info/host, info/admin, and info/access_uri
files are populated, and the slave runs in no-daemon mode while this script
waits. The TERM and HUP signals are trapped to allow this script to terminate
the running slave when the grid scheduler signals for termination.

Author: Michael.V.Pelletier@raytheon.com - November 24, 2013
"""

from __future__ import with_statement
import sys, os, signal, errno, subprocess
from optparse import OptionParser

os.environ['PATH'] = os.pathsep.join(['/bin', '/usr/bin', '/sbin'])
slave_process = None

usage = ("Usage: %prog [options]\n" +
    "\tThe SLAVE_PASSWORD environment variable must provide the password\n" +
    "\tfor the specified slave.")
parser = OptionParser(usage=usage)
parser.add_option("--slavename",
                  help="(required) set buildslave name to SLAVE",
                  metavar="SLAVE")
parser.add_option("--masterhost",
                  help="(required) connect to buildmaster host MASTER",
                  metavar="MASTER")
parser.add_option("--admin", default="",
                  help="set administrator name to ADMIN", metavar="ADMIN")
parser.add_option("--umask", type="int", default=077,
                  help="set buildslave umask to UMASK (default 077)",
                  metavar="UMASK")
parser.add_option("--buildslave", default="/usr/bin/buildslave",
                  help="use EXE as buildslave (default %default)",
                  metavar="EXE")

(options, args) = parser.parse_args()

# Verify mandatory inputs
try:
    options.masterhost
    options.slavename
    os.environ['SLAVE_PASSWORD']
except NameError as option:
    raise NameError('missing command line option: %s' % option)
except KeyError as env_var:
    raise KeyError('Environment variable %s is not defined' % env_var)

def slave_workdir():
    """ Return the slave work directory as a string """
 
    try:
        htcondor = os.environ['_CONDOR_SCRATCH_DIR']
    except KeyError:
        htcondor = False

    try:
        gridengine = os.environ['SGE_ARCH']
    except KeyError:
        gridengine = False

    if not gridengine and not htcondor:
        raise NotImplementedError('I can only run as an HTCondor or ' \
                                  'Grid Engine job')
    try:
        return os.environ['TMPDIR'] + "/" + options.slavename
    except KeyError as env_var:
        raise RuntimeError('%s is not defined; I must be run as a grid job' %
            (env_var))

def _killslave(signum, frame):
    """ Signal handler for buildslave termination """

    if slave_process is not None:
        os.kill(slave_process.pid, signal.SIGKILL)  # Python 2.5 compat
    exit()

def try_mkdir(dirname):
    """ Make a directory, but don't fail if it already exists """
    try:
        os.mkdir(dirname)
    except OSError as mkdir_error:
        if mkdir_error.args[0] is errno.EEXIST:
            pass
        else:
            raise
    except:
        raise

def create_info_files(slave_dir):
    """ Create the slave info/host file

    @param slave_dir: path to the slave work directory
    @type slave_dir: string
    """

    info_dir = slave_dir + '/info/'
    try_mkdir(info_dir)

    def open_info_file(filename):
        return open(info_dir + filename, 'w')

    def _kernel_version():
        return "Kernel version: " + os.uname()[2] + '\n'

    def _total_memory_mb():
        total_mem = 'Total memory (MB): '

        try:
            with open('/proc/meminfo', 'r') as f:
                for line in f.readlines():
                    if 'MemTotal:' in line:
                        total_mem += str(int(line.split( )[1]) / 1024)
                        break
        except:
            total_mem += 'UNKNOWN'

        return total_mem + '\n'

    def _cpu_info():
        cpu_info = 'CPU: '
        try:
            with open('/proc/cpuinfo', 'r') as f:
                for line in f.readlines():
                    if 'model name' in line:
                        cpu_info += ' '.join(line.split( )[3:])
                        break
        except:
            cpu_info += 'UNKNOWN'

        return cpu_info + '\n'

    def _libc_info():
        libc_info = 'Library versions:\n'
        try:
            p = subprocess.Popen(['ldconfig', '-p'], stdout=subprocess.PIPE)
            for line in p.stdout:
                if 'libc.' in line or 'libstdc++' in line:
                    libc_info += line
        except:
            libc_info += '\tUNKNOWN'

        return libc_info

    def _lsb_release():
        try:
            return subprocess.Popen(['lsb_release', '-sd'],
                             stdout=subprocess.PIPE).communicate()[0]
        except:
            pass

    with open_info_file('host') as f:
        f.write(' '.join(["Buildslave:", options.slavename, 'running on',
                          os.uname()[1], 'under', _lsb_release()]))
        f.write(_kernel_version())
        f.write(_total_memory_mb())
        f.write(_cpu_info())
        f.write(_libc_info())

    if options.admin:
        with open_info_file('admin') as f:
            f.write(options.admin + '\n')

    with open_info_file('access_uri') as f:
        f.write('ssh://' + os.uname()[1] + '\n')

def populate_slave_dir(slave_dir):
    """ Run "buildslave create-slave" on the specified directory """

    subprocess.check_call([options.buildslave, 'create-slave',
                           '--allow-shutdown=signal',
                           '--umask=' + options.umask, slave_dir,
                           options.masterhost, options.slavename,
                           os.environ['SLAVE_PASSWORD']])

def buildslave_start(slave_dir):
    """ Start the buildslave in the specified directory """

    return Popen([options.buildslave, 'start --nodaemon', slave_dir])
                    
#=================

os.umask(options.umask)
slave_dir = slave_workdir()
try_mkdir(slave_dir)

populate_slave_dir(slave_dir)
create_info_files(slave_dir)

signal.signal(signal.SIGHUP, _killslave)
signal.signal(signal.SIGTERM, _killslave)

slave_process = buildslave_start(slave_dir)
slave_process.wait()
