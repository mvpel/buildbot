"""
Buildbot latent build slave for the HTCondor scheduler, via the DRMAA
latent buildslave, via the DRMAA-Python bindings and AbstractLatentBuildSlave.

Author: Lital Natan <procscsi@gmail.com>
        trac.buildbot.net/ticket/624 - October 2009
Revisions: Michael Pelletier <michael.v.pelletier@raytheon.com>
           November 2013
"""

from buildbot.buildslave.drmaa import DRMAALatentBuildSlave

class HTCondorLatentBuildSlave(DRMAALatentBuildSlave):

    def __init__(self, slave_start_cmd, *args, **kwargs):
        """
        Dispatch a latent slave to an HTCondor queue

        slave_start_cmd - the OS command used to start the Buildbot slave,
                i.e., a script which creates and runs a non-daemon buildslave
                instance in $_CONDOR_SCRATCH_DIR
        """

        DRMAALatentBuildSlave.__init__(self, slave_start_cmd, *args, **kwargs)
        self.job_template.nativeSpecification = _setup_htcondor_template(self)

    def _start_instance(self):
        """ Start an HTCondor buildslave with the configured options """

        return DRMAALatentBuildSlave._start_instance(self)

    def _setup_htcondor_template(self):
        """ Set up the submit description for an HTCondor latent buildslave. """

        """ SUBMIT DESCRIPTION DIRECTIVES:
        request_buildslave=1 can be used in conjunction with HTCondor config
                to allow us to restrict each node to run only one buildslave
                at a time:
                    MACHINE_RESOURCE_NAMES = buildslave
                    MACHINE_RESOURCE_BUILDSLAVE = 1
                    SLOT_TYPE_1 = buildslave=100%
        accounting_group and _user govern HTCondor dispatch priorities. The
                "buildbot" user should have a low user priority factor
                to avoid inordinate delays in dispatch of the slaves.
        request_cpu, request_disk, and request_memory set the expected peak
                size of the buildslave's operation. For example, if you are
                going to run multiple builds on one slave, or multi-threaded
                tasks, set request_cpu to greater than one
        +JobMaxVacateTime limits the time allowed between the termination
                signal and a hard kill from the scheduler to give the slave
                a reasonable amount of time to close down.
        kill_sig set to SIGHUP allows the buildslave startup script to
                gracefully shut down the slave, via a trap if desired.
        want_graceful_removal insures that the job gets a SIGHUP even if
                the scheduler's config has changed this default.
        getenv=true passes in the basic environment variables to the
                buildslave, including PATH. Not sure whether this is needed.
        periodic_remove allows the scheduler to enforce the missing_timeout,
                removing the job if it hasn't started running by then so that
                it won't start running later while not substantiating.
        arguments passes the slave name to the run_buildslave script to
                be provided to the buildslave setup command.
        environment passes the buildslave password to the slave_start_command
                via the SLAVE_PASSWORD environment variable, preventing
                it from appearing in "ps" output and avoiding hardcoding,
                so each buildslave run can have an unique random password.
        +JobDescription allows the condor_q output to show the slave name.
        """

        return (    "request_buildslave=1\n" +
                    "accounting_group_user=buildbot\n" +
                    "accounting_group=group_developer\n" +

                    "request_cpu=1\n" +
                    "request_disk=2097152\n" +  # Request 2GB disk space
                    "request_memory=2048\n" +   # Request 2GB memory

                    "+JobMaxVacateTime=60\n" +  # Wait 60 sec for slave to end
                    "kill_sig=SIGHUP\n" +       # slave --allow-shutdown=signal
                    "want_graceful_removal=TRUE\n" +
                    "getenv=true\n" +

                    "periodic_remove=(JobStatus!=2 && " +
                        "CurrentTime-EnteredCurrentState > " +
                        self.missing_timeout + ")\n" +

                    "arguments=" + self.slavename + "\n" +
                    "job_ad_information_attrs=BuildslaveName\n" +
                    "+BuildslaveName=\"" + self.slavename + "\"\n" +
                    "+JobDescription=\"" + self.slavename + "\"\n" +
                    "+BuildslaveJob=TRUE\n" +

                    "environment=\"SLAVE_PASSWORD=" +
                         _htcondor_env_value(self.password) + "\"\n"
        )

    def _htcondor_env_value(value):
        """
        Modify the string provided to make it suitable for use as an
        env variable value within the HTCondor "environment" submit
        description directive, by escaping single and double quotes
        and enclosing it in single quotes to protect whitespace.
        """

        value = value.replace('"', '""')
        value = value.replace("'", "''")
        return "'" + value + "'"
