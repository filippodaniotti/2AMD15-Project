import os

from shutil import move, copytree, rmtree
from zipfile import ZipFile
from contextlib import contextmanager
from subprocess import run

from argparse import ArgumentParser, Namespace

from typing import List, Tuple, Dict, Set

@contextmanager
def ondir(path: str):
    cwd = os.getcwd()
    dest = os.path.join(cwd, path)
    os.chdir(dest)
    yield dest
    os.chdir(cwd)
    
@contextmanager
def temp_path(path: str):
    if os.path.isdir(path):
        rmtree(path)
    os.makedirs(path)
    yield
    rmtree(path)

SRC_PATH = os.path.join("src", "2amd15")
JAR_PATH = os.path.join("tools", "GenVec", "GenVec.jar")
SRC_TMP_PATH = os.path.join("app")

SRC_ZIP_NAME = os.path.join("app.zip")
DATA_ZIP_NAME = os.path.join("data.zip")

MAIN_NAME = os.path.join("main.py")
VECTORS_NAME = os.path.join("vectors.csv")

GROUP_NUMBER = 13
NUMBER_OF_COLUMNS = 10000
NUMBER_OF_VECTORS = {
    2: 250,
    3: 1000,
    4: 250
}

SERVER_CONFIGURATION = {
    "ENABLE_EVALUATION": False,
    "ON_SERVER": True
}

END_IMPORT_MARKER = "#_END_IMPORTS\n"
BEGIN_CODE_MARKER = "#_BEGIN_CODE\n"

def write_file_from_list(file: str, lines: List[str]):
    with open(file, "w") as f:
        for line in lines:
            f.write(line)

# def get_conf_files(file: str) -> Tuple[List[str], List[str]]:
#     old_lines, new_lines = [], []
#     with open(file, "r") as f:
#         old_lines = f.readlines()
#         for line_idx, line in enumerate(old_lines):
#             new_lines.append(line)
#             conf_entry = line.split(" ")[0]
#             if conf_entry == "ON_SERVER":
#                 new_lines[line_idx] = "ON_SERVER = True\n"
#             elif conf_entry == "ENABLE_EVALUATION":
#                 new_lines[line_idx] = "ENABLE_EVALUATION = False\n"
            
#     return new_lines, old_lines


class ImportsParser:
    def __init__(self):
        self.imports: Dict[str, Set[str]] = {}
        self.aliases: Dict[str, str] = {}

    def update_imports(self, file: str):
        with open(file, "r") as f:
            for line in f.readlines():
                
                if line == END_IMPORT_MARKER:
                    break
                
                tokens = line.split(" ")
                
                if tokens[0] in ["import", "from"]:
                    _, lib, *_ = tokens
                    lib = lib.strip("\n")
                    if lib not in self.imports:
                        self.imports[lib] = set()
                    
                    if tokens[0] == "from":
                        _, lib, _, *names = tokens
                        # names = [name.strip(",").strip("\n") for name in names]
                        for name in names:
                            name = name.strip(",").strip("\n")
                            self.imports[lib].add(name)
                            
                    if len(tokens) > 2 and tokens[2] == "as":
                        self.aliases[lib] = tokens[3].strip("\n")  
                        
    def __str__(self) -> str:
        out = ""
        for key in self.imports.keys():
            if len(self.imports[key]) > 0:
                out += f"from {key} import"
                for name in self.imports[key]:
                    out += f" {name},"
                out = out[:len(out) - 1] + "\n"
            elif key in self.aliases:
                out += f"import {key} as {self.aliases[key]}\n"
            else:
                out += f"import {key}\n"
        return out
    
    def get_imports(self) -> Dict[str, Set[str]]:
        return self.imports
    
    def get_aliases(self) -> Dict[str, str]:
        return self.aliases
                
def extract_code_lines(file):
    lines = []
    marker_index = -1
    with open(file) as f:
        lines = f.readlines()
        for line_idx, line in enumerate(lines):
            if line == BEGIN_CODE_MARKER:
                marker_index = line_idx
                break
    return lines[marker_index + 1:]

def generate_source_archive(question: int):    
    imports = ImportsParser()
    code_lines: Dict[str, List[str]] = {}
    
    with ondir(SRC_PATH):
        files = filter(
            lambda file: os.path.isfile(file) and ("question" in file or "main" in file), 
            os.listdir()
        )
        for file in files:
            imports.update_imports(file)
            code_lines[file] = extract_code_lines(file)
     
    with temp_path(SRC_TMP_PATH):
        with ondir(SRC_TMP_PATH):
            with open(MAIN_NAME, "w") as f:
                f.write(str(imports))
            with ZipFile(SRC_ZIP_NAME, "w") as zip:
                zip.write(MAIN_NAME)
            move(SRC_ZIP_NAME, "..")
                
    
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
    generate_source_archive(args.question)
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
        help="which question is the submission artifacts about",
    )
    parser.add_argument(
        "-p",
        "--password",
        dest="password",
        type=str,
        required=False,
        help="password of the server, if -a is not passed",
    )
    
    args = parser.parse_args()
    main(args)
