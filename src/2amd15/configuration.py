import os
import sys
import platform

if platform.system() == "Windows":
    os.environ['PYSPARK_PYTHON'] = sys.executable
    os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable 
    
ON_SERVER = False
ENABLE_EVALUATION = True
try:
    from matplotlib import pyplot as plt
except ImportError:
    print("WARNING: matplotlib was not found, evaluation is not enabled")
    ENABLE_EVALUATION = False
