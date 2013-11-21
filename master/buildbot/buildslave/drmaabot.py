"""
Latent buildslave for the Distributed Resource Management Application API
using DRMAA Python bindings: http://code.google.com/p/drmaa-python/downloads
DRMAA Working Group: http://www.drmaa.org/

The buildslave is run by a DRMAA-capable job scheduling system.  When a
slave is requested, it's queued to the scheduler for eventual dispatch to any
execute node, where it starts and then contacts the build master. When the
builds finish, the slave's job is terminated via DRMAA.

Author: Lital Natan <procscsi@gmail.com>
        trac.buildbot.net/ticket/624 - October 2009
Revisions: Michael Pelletier <michael.v.pelletier@raytheon.com>
           November 2013
"""

from twisted.internet import defer, threads
from twisted.python import log
from buildbot.buildslave import AbstractLatentBuildSlave
from buildbot import interfaces, config

try:
    import drmaa
except ImportError:
    drmaa = None

class DRMAALatentBuildSlave(AbstractLatentBuildSlave):

    _drmaa_session = None
    drmaa_session_contact = None
    job_id = None
    job_template = None

    def __init__(self, slave_start_cmd, *args, **kwargs):
        """
        DRMAALatentBuildSlave - start a buildslave with the DRMAA scheduler API
        slave_start_cmd: the OS command which starts the buildslave, typically
                         a script which creates the slave directory and
                         starts a non-daemon buildslave process.
        """

        if not drmaa:
            config.error("Enrico Sirola's 'drmaa' is needed to use a %s" %
                    self.__class__.__name__)

        AbstractLatentBuildSlave.__init__(self, *args, **kwargs)
        self.job_template = self._get_blank_job_template()
        self.job_template.remoteCommand = slave_start_cmd

    def start_instance(self, build):
        """ Start a latent buildslave via a DRMAA-compatible scheduler """

        return threads.deferToThread(self._start_instance)

    def _start_instance(self):
        """ Start a latent buildslave via a DRMAA-compatible scheduler """
        
        drmaa_session = self._get_persistent_drmaa_session()
        self.job_id = drmaa_session.runJob(self.job_template)
        log.msg('%s job %s queued (%s)' %
                (self.drmaa_session_contact, self.job_id, self.slavename))
        return True

    def stop_instance(self, fast=False):
        """ Stop the buildslave DRMAA job designated by job_id """

        # If the instance never started, just return success
        if self.job_id is None:
            return defer.succeed(None)

        log.msg('Stopping %s job %s (%s)' %
                (self.drmaa_session_contact, self.job_id, self.slavename))
        return threads.deferToThread(self._stop_instance)

    def _stop_instance(self):
        """ Stop the buildslave job via DRMAA terminate and clear job_id """
    
        self._terminate_drmaa_job()
        self.job_id = None

    def _terminate_drmaa_job(self):
        """ Terminate the DRMAA job referenced by job_id """

        drmaa_session = self._get_persistent_drmaa_session()
        try:
            drmaa_session.control(self.job_id, drmaa.JobControlAction.TERMINATE)
        except drmaa.errors.InvalidJobException:
            log.msg('%s job %s is no longer valid (%s)' %
                    (self.drmaa_session_contact, self.job_id, self.slavename))
            return
        except:
            log.msg('Terminate error for %s job %s (%s)' %
                    (self.drmaa_session_contact, self.job_id, self.slavename))
            raise

        log.msg('Stopped %s job %s (%s)' %
                (self.drmaa_session_contact, self.job_id, self.slavename))
    
    def _get_persistent_drmaa_session(self):
        """ Obtain or create the DRMAA session instance and return it """

        if not DRMAALatentBuildSlave._drmaa_session:
            session = None
            if self.drmaa_session_contact:
                session = drmaa.Session(self.drmaa_session_contact)
            else:
                session = drmaa.Session()
                self.drmaa_session_contact = session.contact
            DRMAALatentBuildSlave._drmaa_session = session

        return DRMAALatentBuildSlave._drmaa_session

    def _get_blank_job_template(self):
        """ Create and return a blank DRMAA job template """

        drmaa_session = self._get_persistent_drmaa_session()
        return drmaa_session.createJobTemplate()
