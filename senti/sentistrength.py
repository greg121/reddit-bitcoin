#!/usr/bin/python
# -*- coding: utf-8 -*-

import subprocess


def senti(string):
    try:
        result =  subprocess.check_output(['java', '-jar', '/home/gregor/reddit/senti/SentiStrength.jar', 'sentidata', '/home/gregor/reddit/senti/SentStrength_Data/', 'text', string])
    except Exception, e:
        result = str(e.output)
    return result
