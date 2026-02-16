import sys
import time

print("Hello World, and there is a file: " + sys.argv[1] + "\n", flush=True)

time.sleep(5)

print("Half-time: " + sys.argv[1] + "\n", flush=True)

time.sleep(5)

print("And we are done with: " + sys.argv[1] + "\n", flush=True)
