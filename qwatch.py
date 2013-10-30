#!/usr/bin/env python

"""
Tool to watch the PBS queue status

This tool shows the current PBS queue contents as a table, which is
(if desired) updated every two seconds. Auto update can be toggled
by hitting (a). By default, all running jobs including walltime and
memory usage are shown. Hitting (u) toggles the user switch, with which
only those jobs are shown whose owner is the current unix user.

Runs from python 2.4. The tool qstat of the PBS toolchain must be installed.
"""

import curses
from xml.dom import minidom
import subprocess
import threading
import getpass


__author__ = "Jan Oliver Oelerich"
__copyright__ = "Copyright 2013, Universitaet Marburg"
__credits__ = ["Jan Oliver Oelerich"]
__license__ = "GPL"
__version__ = "1.0.0"
__maintainer__ = "Jan Oliver Oelerich"
__email__ = "jan.oliver.oelerich@physik.uni-marburg.de"
__status__ = "Production"


class Job(object):
    """
    Class, that represents one job and acts as a container for the
    information from the qstat -x command.
    """

    def __init__(self, data):
        """Init the job and set the data"""
        self.data = data

    def d(self, path):
        """Get some piece of information by specifying the xml
        path relative to <Job> of the qstat -x output."""
        a = self.data
        for segment in path.split('.'):
            a = a[segment]
        return a

    def __getattr__(self, name):
        """Magic getter function. Those getters defined by the
        mapping below are known, all others raise an exception"""
        property_map = {
            'name': 'job_name',
            'id': 'job_id',
            'owner': 'job_owner',
            'time': 'resources_used.walltime',
            'memory': 'resources_used.mem',
            'queue': 'queue',
            'host': 'exec_host'
        }
        try:
            return self.d(property_map[name])
        except:
            raise

    def get_memory(self):
        """Format the memory usage in MB/KB/GB"""
        mem = int(self.d('resources_used.mem')[:-2])

        if mem > 1024 * 1024:
            return '%.1f GB' % (mem / (1024. * 1024.))
        if mem > 1024:
            return '%.1f MB' % (mem / 1024.)

        return '%.1f kB' % (mem / 1024.)

    def get_owner(self):
        """Get rid of the @host in the owner name"""
        o = self.d('job_owner')
        return o[:o.find('@')]

    memory = property(get_memory, None, None, "Formatted memory.")
    owner = property(get_owner, None, None, "Formatted owner.")


class QWatchParser(object):
    """
    This function parses the output of qstat -x and returns an array of
    job objects.
    """
    def __init__(self):
        pass

    def parse(self, xml):
        """Expects xml as a string, creates a list of jobs"""
        doc = minidom.parseString(xml)
        jobs = []
        for job in doc.getElementsByTagName("Job"):
            j = Job(self.parse_job(job))
            jobs.append(j)
        return jobs

    def parse_job(self, job_element):
        """Converts the xml tree under <Job> into a nested
        dictionary and returns that."""
        data = dict()

        for node in job_element.childNodes:
            key = self.normalize_name(node.nodeName)
            if node.childNodes[0].nodeType == node.childNodes[0].TEXT_NODE:
                data[key] = node.childNodes[0].nodeValue
            elif len(node.childNodes) > 0:
                data[key] = self.parse_job(node)

        return data

    @staticmethod
    def normalize_name(name):
        return name.lower()


class QWatch(object):
    def __init__(self, screen):
        self.parser = QWatchParser()
        self.scr = screen
        self.jobs = []
        self.work_timer = None

        self.settings_own = False
        self.settings_auto_refresh = True

    def start(self):
        self.mainloop()

    def display_header(self):
        # clear first two lines
        self.scr.move(0, 0)
        self.scr.clrtoeol()
        self.scr.move(1, 0)
        self.scr.clrtoeol()

        # write the header
        self.scr.move(0, 0)
        self.scr.addstr("[ ] ")
        self.scr.addstr("a", curses.A_UNDERLINE)
        self.scr.addstr("uto refresh        [ ] ")
        self.scr.addstr("u", curses.A_UNDERLINE)
        self.scr.addstr("ser's jobs        ")
        self.scr.addstr("r", curses.A_UNDERLINE)
        self.scr.addstr("efresh        ")
        self.scr.addstr("q", curses.A_UNDERLINE)
        self.scr.addstr("uit")
        self.scr.addstr(1, 0, '%-20s' % 'Owner', curses.A_STANDOUT)
        self.scr.addstr(1, 15, '%-20s' % 'JOB ID', curses.A_STANDOUT)
        self.scr.addstr(1, 35, '%-20s' % 'JOB Name', curses.A_STANDOUT)
        self.scr.addstr(1, 55, '%-10s' % 'Queue', curses.A_STANDOUT)
        self.scr.addstr(1, 65, '%-10s' % 'Node', curses.A_STANDOUT)
        self.scr.addstr(1, 75, '%-10s' % 'Time', curses.A_STANDOUT)
        self.scr.addstr(1, 85, '%-10s' % 'Memory', curses.A_STANDOUT)

        # show settings
        self.scr.addstr(0, 1, self.setting_display(self.settings_auto_refresh))
        self.scr.addstr(0, 25, self.setting_display(self.settings_own))

        # refresh display and restart the timer
        self.scr.refresh()

        if self.work_timer:
            self.work_timer.cancel()
        self.refresh_data()

    def mainloop(self):

        self.display_header()

        while 1:
            c = self.scr.getch()
            if c == ord('q'):
                self.work_timer.cancel()
                break
            elif c == ord('a'):
                self.settings_auto_refresh = not self.settings_auto_refresh
                self.display_header()
            elif c == ord('r'):
                self.display_header()
            elif c == ord('u'):
                self.settings_own = not self.settings_own
                self.display_header()

    def refresh_data(self):
        if self.settings_auto_refresh:
            self.work_timer = threading.Timer(2.0, self.refresh_data)
            self.work_timer.start()

        proc = subprocess.Popen(['qstat', '-x'], stdout=subprocess.PIPE)
        self.jobs = self.parser.parse(proc.communicate()[0])

        self.scr.move(2, 0)
        self.scr.clrtobot()

        jobs = self.get_jobs()

        if len(jobs):
            for i, job in enumerate(jobs):
                self.scr.addnstr(i + 2, 0, job.owner, 19)
                self.scr.addnstr(i + 2, 15, job.id, 19)
                self.scr.addnstr(i + 2, 35, job.name, 19)
                self.scr.addnstr(i + 2, 55, job.queue, 9)
                self.scr.addnstr(i + 2, 65, job.host, 9)
                self.scr.addnstr(i + 2, 75, job.time, 9)
                self.scr.addnstr(i + 2, 85, job.memory, 9)
        else:
            self.scr.addstr(2, 20, "Currently no jobs in the queue.")

        self.scr.refresh()

    def get_jobs(self):
        ret = list()
        for j in self.jobs:
            if not self.settings_own or j.owner == getpass.getuser():
                ret.append(j)
        return ret

    @staticmethod
    def setting_display(setting):
        if setting:
            return 'x'
        return ' '


def main(screen):
    qwatch = QWatch(screen)
    qwatch.start()


if __name__ == '__main__':
    curses.wrapper(main)
