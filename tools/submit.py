import os

from shutil import move, copytree, rmtree
from zipfile import ZipFile
from contextlib import contextmanager
from subprocess import run

from argparse import ArgumentParser, Namespace

from typing import List, Tuple

@contextmanager
def ondir(path: str):
    cwd = os.getcwd()
    dest = os.path.join(cwd, path)
    os.chdir(dest)
    yield dest
    os.chdir(cwd)

SRC_PATH = os.path.join("src", "2amd15")
JAR_PATH = os.path.join("tools", "GenVec", "GenVec.jar")
SRC_TMP_PATH = os.path.join("app")

SRC_ZIP_NAME = os.path.join("app.zip")
DATA_ZIP_NAME = os.path.join("data.zip")
VECTORS_NAME = os.path.join("vectors.csv")

GROUP_NUMBER = 13
NUMBER_OF_COLUMNS = 10000
NUMBER_OF_VECTORS = {
    2: 250,
    3: 1000,
    4: 250
}

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

def generate_source_archive():
    if os.path.isdir(SRC_TMP_PATH):
        rmtree(SRC_TMP_PATH)
    copytree(SRC_PATH, SRC_TMP_PATH)
    with ondir(SRC_TMP_PATH):
        files = list(filter(lambda file: os.path.isfile(file), os.listdir()))
        with ZipFile(SRC_ZIP_NAME, "w") as zip:
            for file in files:
                if file == "configuration.py":
                    server_conf, _ = get_conf_files(file)
                    write_file_from_list(file, server_conf)
                zip.write(file)
    move(os.path.join(SRC_TMP_PATH, SRC_ZIP_NAME), SRC_ZIP_NAME)
    rmtree(SRC_TMP_PATH)
    
def generate_data_archive(question: int):
    with ZipFile(DATA_ZIP_NAME, "w") as zip:
        run([
            "java", 
            "-jar", 
            JAR_PATH, 
            str(GROUP_NUMBER), 
            str(NUMBER_OF_VECTORS[question]), 
            str(NUMBER_OF_COLUMNS)
        ])
        zip.write(VECTORS_NAME)

def main(args: Namespace):
    generate_data_archive(args.question)
    # generate_source_archive()
    if not args.artifact_only:
        pass
        # submit to the server
            
if __name__ == "__main__":
    parser = ArgumentParser(description="Handles the building pipeline of the submission artifact.")
    parser.add_argument(
        "-a",
        "--artifact-only",
        action="store_true",
        dest="artifact_only",
        required=False,
        help="stop before submitting the artifacts to the server"
    )
    parser.add_argument(
        "-q",
        "--question",
        dest="question",
        type=int,
        required=True,
        choices=range(2, 5),
        help="path of the video to analyze",
    )
    
    args = parser.parse_args()
    main(args)
