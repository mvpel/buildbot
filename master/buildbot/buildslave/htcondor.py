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
        r""" Dispatch a latent buildslave to an HTCondor queue
        
        @param slave_start_cmd: the OS command used to start the buildslave on
                                the execute node on which the job runs.
        @param accounting_group: The name of the HTCondor accounting group;
                                 requires HTCondor v8.x or higher
        @param accounting_group_user: name of the accounting group user;
                                      requires HTCondor v8.x or higher
        @param request_buildslave: with v7.9.0 machine_resource_names you can
                                   specify a machine resource in your HTCondor
                                   config to limit the number of buildslaves
                                   which can run on a single exec host:
                                     MACHINE_RESOURCE_NAMES = \
                                          $(MACHINE_RESOURCE_NAMES) Buildslave
                                     MACHINE_RESOURCE_Buildslave = 1
                                     SLOT_TYPE_1 = buildslave=100%
                                   Use "machine_resource_inventory_buildslave"
                                   to specify a script which detects if a
                                   machine has Buildbot available and returns
                                   the desired maximum number of slaves per
                                   host.  If none, it must still report zero:
                                     DetectedBuildslave=0
                                   This parameter can't be used unless this
                                   HTCondor configuration is in place.
        @param request_cpus: reserve this many CPU cores for the buildslave
        @param request_disk: request the use of the specified number of
                             KILOBYTES of disk space in the exec host's
                             $_CONDOR_SCRATCH_DIR scratch directory, default
                             of 2GB.
        @param request_memory: request the specified number of MEGABYTES of
                               memory for the buildslave
        @param extra_submit_descr: a newline-separated additional entries for
                                   the HTCondor submit description file used to
                                   start the buildslave.

        Each buildslave job sets the ClassAd "BuildslaveJob", which can be used
        in rank and requirements expressions.

        A periodic_remove expression is specified to instruct HTCondor delete
        the job if it goes on hold, or has been idle for longer the latent
        slave's missing_timeout.
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
            'request_buildslave'    :None,  # HTCondor machine resource config

            'request_cpu'           :'1',
            'request_disk'          :'2097152',  # 2GB scratch space default
            'request_memory'        :'2048',  # 2GB memory default
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

        Modify the string provided to make it suitable for use as an
        env variable value within the HTCondor "environment" submit
        description directive, by escaping single and double quotes
        and enclosing the result in single quotes.
        """

        value = value.replace('"', '""')
        value = value.replace("'", "''")
        return "'" + value + "'"
