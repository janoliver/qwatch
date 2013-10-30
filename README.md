# Tool to watch the PBS queue status

This tool shows the current PBS queue contents as a table, which is
(if desired) updated every two seconds. Auto update can be toggled
by hitting (a). By default, all running jobs including walltime and
memory usage are shown. Hitting (u) toggles the user switch, with which
only those jobs are shown whose owner is the current unix user.

Runs from python 2.4. The tool `qstat` of the PBS toolchain must be installed.
