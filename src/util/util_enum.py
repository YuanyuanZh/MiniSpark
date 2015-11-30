#!/usr/bin/env python

"""
This file define all enum information of this project.
If you want to know more detail, please read the README.md
"""

from enum import Enum

# Event enum variable
Event = Enum('Event','REGISTER NEW_JOB FINISH_TASK WORKER_DOWN FINISH_JOB')

# Task status enum variable
Status = Enum('Status', 'INITIAL START PROCESSING FINISH FINISH_REPORTED')

# Worker status enum variable
Worker_Status = Enum('Worker_Status', 'UP DOWN')