Hi there! This short readme is supposed to accompany a file called GenVec.jar.
Genvec.jar can be used to generate a dataset containing labelled vectors for the 2AMD15 project.

GenVec takes one required- and two optional runtime parameters, namely:
* your group number (without leading a leading zero, so 9 instead of 09)
* the number of vectors to generate (default value is 40)
* the length of each vector (default value is 20)
	
You can run GenVec, after installing an up-to-date version of Java, by running the following command:
```bash
java -jar group-number [number-of-vectors] [length-of-vectors]
``` 
	
from a command line in the directory which contains GenVec.jar. This will (over)write a file named vectors.csv in the same directory.

The contents of vector.csv should be the same each time you run GenVec with the same runtime parameters.
If this is not the case, please let us know.