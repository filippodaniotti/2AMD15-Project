import os

from shutil import move
from zipfile import ZipFile
from contextlib import contextmanager

@contextmanager
def ondir(path: str):
    cwd = os.getcwd()
    dest = os.path.join(cwd, path)
    os.chdir(dest)
    yield dest
    os.chdir(cwd)

SRC_PATH = os.path.join("src", "2amd15")
ZIP_NAME = os.path.join("app.zip")

def main():
    with ondir(SRC_PATH):
        files = list(filter(lambda file: os.path.isfile(file), os.listdir()))
        with ZipFile(ZIP_NAME, "w") as zip:
            print(list(files))
            for file in files:
                print(file)
                # if file == "configuration.py":
                #     print("yo")
                #     with open(file, "r") as f:
                #         print(f.read())
                zip.write(file)
    move(os.path.join(SRC_PATH, ZIP_NAME), ZIP_NAME)
            
            
def set_configuration():
    pass

if __name__ == "__main__":
    main()