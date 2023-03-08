import os

from shutil import move
from zipfile import ZipFile
from contextlib import contextmanager

from typing import List, Tuple

@contextmanager
def ondir(path: str):
    cwd = os.getcwd()
    dest = os.path.join(cwd, path)
    os.chdir(dest)
    yield dest
    os.chdir(cwd)

SRC_PATH = os.path.join("src", "2amd15")
ZIP_NAME = os.path.join("app.zip")

def write_file_from_list(file: str, lines: List[str]):
    with open(file, "w") as f:
        for line in lines:
            f.write(line)

def get_conf_files(file: str) -> Tuple[List[str], List[str]]:
    old_lines, new_lines = [], []
    with open(file, "r") as f:
        old_lines = f.readlines()
        for line_idx, line in enumerate(old_lines):
            new_lines.append(line)
            conf_entry = line.split(" ")[0]
            if conf_entry == "ON_SERVER":
                new_lines[line_idx] = "ON_SERVER = True\n"
            elif conf_entry == "ENABLE_EVALUATION":
                new_lines[line_idx] = "ENABLE_EVALUATION = False\n"
            
    return new_lines, old_lines

def main():
    with ondir(SRC_PATH):
        files = list(filter(lambda file: os.path.isfile(file), os.listdir()))
        with ZipFile(ZIP_NAME, "w") as zip:
            for file in files:
                if file != "configuration.py":
                    zip.write(file)
                elif file == "configuration.py":
                    server_conf, local_conf = get_conf_files(file)
                    write_file_from_list(file, server_conf)
                    zip.write(file)
                    write_file_from_list(file, local_conf)
    move(os.path.join(SRC_PATH, ZIP_NAME), ZIP_NAME)
            
if __name__ == "__main__":
    main()