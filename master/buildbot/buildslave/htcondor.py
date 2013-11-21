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
        
        slave_start_cmd -- the OS command used to start the Buildbot slave,
            i.e., a script which creates and runs a non-daemon buildslave
            instance in $_CONDOR_SCRATCH_DIR. The script is transferred to
            the exec node, so need not be on a shared filesystem.
        accounting_group
        accounting_group_user -- name of the HTCondor accounting user or group
            to which buildbot jobs should be assigned. This syntax requires
            v8.x; if you haven't upgraded yet, use extra_submit_description to
            specify an "+AccountingGroup" ClassAd. The priority factor of
            the buildbot user or group should be kept low in order to insure
            that queued buildslave jobs aren't excessively delayed.
        request_buildslave -- in conjunction with v7.9.0 machine_resource_names
            you can specify a resource in your HTCondor config to limit the
            number of buildslaves which can run on a single host:
                MACHINE_RESOURCE_NAMES = $(MACHINE_RESOURCE_NAMES) Buildslave
                MACHINE_RESOURCE_Buildslave = 1
                SLOT_TYPE_1 = buildslave=100%
            You can also use "machine_resource_inventory_buildslave" to provide
            a script which detects if a machine has Buildbot available and
            returns the desired number. If none, it must still report zero:
                DetectedBuildslave=0
            This option can't be used unless this configuration is in place.
        request_cpus -- reserve this many CPU cores for use by the buildslave.
            Since this is set when the slave is created and doesn't change,
            most slaves should use the default of 1 and the builds should
            be designed to make full use of one core; in particular, max_builds
            should always be set to 1. If you have builders which require
            multiple cores for a build, then you should create separate latent
            slaves with larger request_cpus for use by only those builders,
            in order to avoid wasting pool resources.
        request_disk -- request the use of the specified number of KILOBYTES
            of disk space in the exec node's scratch directory, default of
            2GB. The location is provided to the Buildslave in the
            $_CONDOR_SCRATCH_DIR and $TMPDIR env variables. To use this
            space, the env variables must be referenced in the build steps,
            and it is cleaned up automatically when the slave ends so the
            builders don't need to do it. Size variances
        request_memory -- request the specified number of MEGABYTES of memory
            for use by the latent buildslave. This is set at creation of the
            slave and can't be adjusted by the build, so select a reasonable
            value that can accommodate the expected range of values for the
            attached builders. You can use the condor_history command to
            review the memory use of prior slave runs.
        extra_submit_description -- a string containing newline-separated
            additional entries for the HTCondor submit description file for
            the buildslave. This must be used for job ClassAds with "+AdName"
            notation, since a "+" can't be used in a keyword arg.

        In addition, each buildslave job sets the ClassAd "BuildslaveJob",
        which can be used in rank and requirements expressions. For example,
        to prevent a machine from running any buildslaves, change the START
        expression for that machine to refuse the jobs:
            START = $(START) && (TARGET.BuildslaveJob =!= True)

        A periodic_remove expression is specified to have HTCondor delete the
        job if it goes on hold or has been idle for longer the latent slave's
        missing_timeout, to prevent it from being dispatched later and then
        contacting the master when it is not substantiating the slave.
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
            kwargs['extra_submit_description']
        except:
            pass
        else:
            native_spec += kwargs['extra_submit_description'] + "\n"

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
