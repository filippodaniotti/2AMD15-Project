# 2AMD15-Project

## How to use

### Prepare for submission

All the code is inside the `src/2amd15` folder. In order to prepare for the submission, a `app.zip` needs to be uploaded to the server, and such archive should contain a `main.py` file.

To produce the `app.zip` archive, you can run the following:

* Unix
```bash
python3 ./tools/prepare_for_submission.py
```
* Win
```cmd
python .\tools\prepare_for_submission.py
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
