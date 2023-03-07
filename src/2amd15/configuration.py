import os
import sys
import platform

if platform.system() == "Windows":
    os.environ['PYSPARK_PYTHON'] = sys.executable
    os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable 
    


ENABLE_EVALUATION = True

