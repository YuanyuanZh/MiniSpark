#!/usr/bin/env python

"""
This file define all enum information of this project.
If you want to know more detail, please read the README.md
"""

from enum import Enum

# Event enum variable
#Event = Enum('Event','REGISTER NEW_JOB FINISH_TASK WORKER_DOWN FINISH_JOB')

# Task status enum variable
#Status = Enum('Status', 'INITIAL START PROCESSING FINISH FINISH_REPORTED')

# Worker status enum variable
# Worker_Status = Enum('Worker_Status', 'UP DOWN')


class Worker_Status(Enum):
   UP = 1
   DOWN = 2


class Status(Enum):
    INITIAL = 1
    START = 2
    PROCESSING = 3
    FINISH = 4
    FINISH_REPORTED = 5
    FAIL = 6

class Event(Enum):
    REGISTER = 1
    NEW_JOB = 2
    FINISH_TASK = 3
    WORKER_DOWN = 4
    FINISH_JOB = 5
    ASSIGN_TASK = 6