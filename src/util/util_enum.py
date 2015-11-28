#!/usr/bin/env python

"""
This file define all enum information of this project.
If you want to know more detail, please read the README.md
"""

from enum import Enum

# Event enum variable
Event = Enum('REGISTER', 'NEWJOB', 'FINISHTASK', 'WORKERDOWN', 'FINISHJOB')
