#!/home/nivag/2023 - Linux/.python-3.10-1/bin/python

import subprocess
import pexpect
import sys
import time
             
child = pexpect.spawn('sudo mkdir test', timeout=30)
print(child.read())