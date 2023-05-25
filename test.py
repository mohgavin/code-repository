#!/home/nivag/2023 - Linux/.python-3.10-1/bin/python

import subprocess
import pexpect
import sys
import time
             
child = pexpect.spawn('bash', encoding='utf-8', logfile=sys.stdout)

child.sendline('cd /home/nivag/2023 - Linux')
child.expect("")

child.sendline('cp plotly_test.ipynb plot1.ipynb')
child.send("\r")

child.expect("halo")

print(child.before)