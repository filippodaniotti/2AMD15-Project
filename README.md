# 2AMD15-Project

## How to use

### Prepare for submission

All the code is inside the `src/2amd15` folder. In order to prepare for the submission, a `app.zip` needs to be uploaded to the server, and such archive should contain a `main.py` file. The data should be contained in a `data.zip` archive. Only one question per submission can be uploaded and tested.

The `submit.py` module takes care of prepare the aforementioned submission artifacts. Here is how to use it:
```
usage: submit.py [-h] [-s] -q {2,3,4} [-p PASSWORD] [-v] [-r ROWS] [-c COLS] [-f]

Handles the building pipeline of the submission artifact.

options:
  -h, --help            show this help message and exit
  -s, --submit          Proceed with creating a submission on the server after build
  -q {2,3,4}, --question {2,3,4}
                        which question is the submission artifacts about
  -p PASSWORD, --password PASSWORD
                        password of the server, required if -s is passed
  -v, --verbose         verbose flag, sets logging level to debug
  -r ROWS, --rows ROWS  number of vectors in the csv, overwrites default
  -c COLS, --columns COLS
                        length of vectors in the csv, overwrites default
  -f, --full-build      builds a main.py with full code
```

For instance, here is how you prepare the artifacts and submit for question 2:

* Unix
```bash
python3 ./tools/submit.py -q 2 -s
```
* Win
```cmd
python .\tools\submit.py -q 2 -s
```

### Connect to server
It is possible to connect to the server via `sft` with the following script and then typing in the password
* Unix
```bash
./tools/server/connect.sh
```
* Win
```cmd
.\tools\server\connect.bat
```

## Template information
This is a template project for team project of 2AMD15 2023.

Various comments in the main.py file indicate where functionality needs to be implemented.
These comments are marked TODO.

You are allowed to change the layout of the main.py file (such as method names and signatures) in any way you like,
and you can add new functionality (possibly in new .py files). Do make sure that there exists a file called "main.py"
which contains the 'main module statement' of the form "if __name__ == '__main__':".

You can ZIP the main.py file along with any other .py files using any common compression tool. The archive should
be called app.zip and can be uploaded as such to the server.

Good luck on the project!
