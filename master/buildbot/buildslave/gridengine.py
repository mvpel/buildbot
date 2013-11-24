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
"""Latent BuildSlave for the Grid Engine Distributen System

Author: Lital Natan <procscsi@gmail.com>
        trac.buildbot.net/ticket/624 - October 2009
"""

from buildbot.buildslave.drmaa import DRMAALatentBuildSlave

class OptionStub(object):
    def __init__(self, option_name, option_dict):
        self.option_name = option_name
        self.option_dict = option_dict

    def setter(self, value):
        self.option_dict[self.option_name] = value

    def getter(self):
        return self.option_dict.get(self.option_name, None)


class GridEngineLatentBuildSlave(DRMAALatentBuildSlave):
    """Latent BuildSlave for the Grid Engine Distributen System"""

    # Dictionary for naming SGE job arguments in a 'human-readable' form
    grid_option_dictionary = {
            'queue':'-q',
            'name':'-N',
            'shell':'-S',
            'env': '-v',
            }

    job_options = None # SGE job options (dict[argument]=value)
    job_resources = None # SGE job resources (dict[resource]=amount/object)

    def __init__(self, buildslave_setup_command, *args, **kwargs):
        """Default Constructor

        buildslave_setup_command -- the o/s command starting the BuildBot slave
                                    its recommended to use a command starting a 
                                    non-daemon slave instance.
        *args -- non-keyword arguments for the BuildBot slave
        **kwargs -- keyword arguments for the BuildBot slave

        """
        # Default job options
        self.job_options = {
                'queue': 'all.q',
                'name': 'BuildSlave',
                }

        # Default job resources
        self.job_resources = {
                'arch': '*',
                }

        DRMAALatentBuildSlave.__init__(self, buildslave_setup_command, *args, **kwargs)

        # Create option stubs for all the job parameters 'named' by 'grid_options_dictionary'
        for option_name, option_arg in self.grid_option_dictionary.iteritems():
            option_stub = OptionStub(option_name, self.job_options)
            setattr(self, "get_job_" + option_name, option_stub.getter)
            setattr(self, "set_job_" + option_name, option_stub.setter)

    def _start_instance(self):
        """Startes a BuildSlave instance with the set job options and resources"""

        native_specification_string = ""
        # Construct job options in to string
        for option_name, option_value in self.job_options.iteritems():
            option_arg = self.grid_option_dictionary[option_name]
            native_specification_string += option_arg + " " + option_value + " "
        # Construct job resources in to string
        resource_specification_string = ""
        for res_name, res_value in self.job_resources.iteritems():
            if resource_specification_string:
                resource_specification_string += ","
            resource_specification_string += res_name + "=" + res_value
        if resource_specification_string:
            native_specification_string += "-l " + resource_specification_string
        self.job_template.nativeSpecification = native_specification_string
        return DRMAALatentBuildSlave._start_instance(self)
