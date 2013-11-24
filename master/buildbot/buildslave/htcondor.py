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
Buildbot latent build slave for the HTCondor scheduler, via the DRMAA
latent buildslave, via the DRMAA-Python bindings and AbstractLatentBuildSlave.

Author: Lital Natan <procscsi@gmail.com>
        trac.buildbot.net/ticket/624 - October 2009
Revisions: Michael Pelletier <michael.v.pelletier@raytheon.com>
           November 2013
"""

from buildbot.buildslave.drmaabot import DRMAALatentBuildSlave

class HTCondorLatentBuildSlave(DRMAALatentBuildSlave):

    def __init__(self, slave_start_cmd, *args, **kwargs):
        """ Dispatch a latent buildslave to an HTCondor queue
        
        @param slave_start_cmd: the OS command used to create and start the
            buildslave on the execute node to which the job is dispatched by
            HTCondor, which must accept slave name as first argument, and use
            BUILDSLAVE_PASSWORD environment variable for slave password.
        @param accounting_group: The priority accounting group; requires
            HTCondor v8.x.
        @param accounting_group_user: priority accounting username (default:
            buildmaster process owner); requires HTCondor v8.x if specified.
        @param request_buildslave: with v7.9.0 C{machine_resource_names} you
            may specify a C{buildslave} resource in your HTCondor config to
            limit the number of buildslaves which can run on a single host::

                MACHINE_RESOURCE_NAMES = $(MACHINE_RESOURCE_NAMES) Buildslave
                MACHINE_RESOURCE_Buildslave = 1
                SLOT_TYPE_1 = buildslave=100%
                
            Use C{machine_resource_inventory_buildslave} to specify a script
            which detects if a machine has Buildbot available and prints
            a string assigning the desired maximum number of slaves per host
            to C{DetectedBuildslave}. If none, it must assign zero::

                DetectedBuildslave=0

            This parameter can't be used unless the machine resource is set up.
        @type request_buildslave: integer
        @param request_cpus: reserve this many CPU cores for the buildslave
        @type request_cpus: integer
        @param request_disk: request the specified number of I{kilobytes}
            of disk space in the exec host's scratch directory referenced by
            job environment variables C{$_CONDOR_SCRATCH_DIR} and C{$TMPDIR}
            (default 1GB)
        @type request_disk: integer kilobytes
        @param request_memory: request the specified number of I{megabytes} of
            memory for the buildslave.
        @type request_memory: integer megabytes
        @param extra_submit_descr: newline-separated additional entries for
            the HTCondor submit description file used to start the buildslave.
        @param extra_submit_descr: string

        Each buildslave job sets the boolean ClassAd "BuildslaveJob", which can
        be used in rank and requirements expressions.

        A periodic_remove expression is defined to instruct HTCondor delete
        the job if it goes on hold, or has been idle for longer than the latent
        slave's C{missing_timeout} parameter.
        """

        DRMAALatentBuildSlave.__init__(self, slave_start_cmd, *args, **kwargs)
        self.job_template.nativeSpecification = \
                self._setup_htcondor_template(self, **kwargs)

    def _start_instance(self):
        """ Deferred instance start for an HTCondor buildslave job """

        return DRMAALatentBuildSlave._start_instance(self)

    def _setup_htcondor_template(self, **kwargs):
        """ Set up the submit description for an HTCondor latent buildslave """

        submit_description_rw = {
            'accounting_group'      :None,  # Site-specific; needs HTCondor 8.x
            'accounting_group_user' :None,  # Defaults to process owner
            'request_buildslave'    :None,  # HTCondor machine_resource

            'request_cpu'           :'1',
            'request_disk'          :'1048576', # 1GB scratch space default
            'request_memory'        :'2048',    # 2GB memory default
        }

        submit_description_ro = {
            'arguments'         :self.slavename,
            '+BuildslaveName'   :self.slavename,
            '+JobDescription'   :self.slavename,
            'environment'       :('SLAVE_PASSWORD='
                + _htcondor_env_value(str(self.password))),
            '+BuildslaveJob'    :'TRUE',
            '+JobMaxVacateTime' :'60',
            'getenv'            :'TRUE',
            'kill_sig'              :'SIGHUP',
            'want_graceful_removal' :'TRUE',
            'transfer_executable'   :'TRUE',    # Transfer slave_start_cmd

            # Remove the job if it goes on hold or stays idle for longer
            # than missing_timeout. Minimum of 2 minutes due to negotiator
            # interval.
            'periodic_remove'   :('(JobStatus == 5 || (JobStatus == 1 &&' +
                'CurrentTime - QDate > ' +
                str(120 if self.missing_timeout<120 else self.missing_timeout)
                + '))'),
        }

        # Update configurable directives
        for k,v in kwargs.iteritems():
            if k in submit_description_rw.keys():
                submit_description_rw[k] = str(v)

        # Generate DRMAA Native Specification string suitable for HTCondor
        native_spec = ""
        for k,v in submit_description_rw.iteritems():
            if submit_description_rw[k] is not None:
                native_spec += k + "=" + v + "\n"

        # Add the user-defined submit description directives
        try:
            kwargs['extra_submit_descr']
        except:
            pass
        else:
            native_spec += kwargs['extra_submit_descr'] + "\n"

        # Add the read-only directives last so they can't be overridden,
        # as the proper operation of the slave depends on some of them
        for k,v in submit_description_ro.iteritems():
            native_spec += k + "=" + v + "\n"

        return native_spec

    def _htcondor_env_value(value):
        """ Convert a string to an HTCondor environment variable value

        @param value: string to be assigned to an environment variable in an
            HTCondor C{environment} submit descriptiondirective.
        @type value: string
        
        Returns the string enclosed in single quotes with single and double
        quotes escaped according to HTCondor submit descriptionrequirements. 
        """

        value = value.replace('"', '""')
        value = value.replace("'", "''")
        return "'" + value + "'"
