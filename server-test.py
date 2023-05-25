import subprocess
import pexpect
import sys
import time
             
child = pexpect.spawn('sudo mkdir test', timeout=30)
print(child.read())