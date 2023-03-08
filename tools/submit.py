import os
import re

import pysftp
from subprocess import run
from zipfile import ZipFile
from contextlib import contextmanager
from argparse import ArgumentParser, Namespace

from typing import List, Dict, Set

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

HOSTNAME = "odc-09.win.tue.nl"
USERNAME = "group-13-2amd15-23"
PORT = 222

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
                
def extract_code_lines(file: str) -> List[str]:
    lines = []
    marker_index = -1
    with open(file) as f:
        lines = f.readlines()
        for line_idx, line in enumerate(lines):
            if line == BEGIN_CODE_MARKER:
                marker_index = line_idx
                break
    return lines[marker_index + 1:]

def parse_configuration(file_lines: List[str]):
    for line_idx, line in enumerate(file_lines):
        for conf in SERVER_CONFIGURATION.keys():
            if "configuration." in line and conf in line:
                file_lines[line_idx] = line.replace(f"configuration.{conf}", str(SERVER_CONFIGURATION[conf]))
    return file_lines

def build_main(
    question: int, 
    code_lines: Dict[str, List[str]],
    file_names: List[str],
    imports: ImportsParser):
    
    question_file, main_file = file_names
    
    with open(MAIN_NAME, "w") as f:
        f.write(str(imports))
        for line in code_lines[question_file]:
            f.write(line)
        for line in code_lines[main_file]:
            if re.match("^(\s)*q[2-4]", line) and f"q{question}" not in line:
                line = "#" + line
            f.write(line)

def generate_source_archive(question: int):    
    file_names = [f"question{question}.py", MAIN_NAME]
    
    imports = ImportsParser()
    code_lines: Dict[str, List[str]] = {}
    
    with ondir(SRC_PATH):
        if (file_names[0] not in os.listdir() or
            file_names[1] not in os.listdir()):
            raise RuntimeError("Missing files in source directory")
        for file in file_names:
            imports.update_imports(file)
            lines = extract_code_lines(file)
            lines = parse_configuration(lines)
            code_lines[file] = lines
     
    build_main(question, code_lines, file_names, imports)
    with ZipFile(SRC_ZIP_NAME, "w") as zip:
        zip.write(MAIN_NAME)
    if os.path.isfile(MAIN_NAME):
        os.remove(MAIN_NAME)
                    
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
        raise NotImplementedError("Currently not supported")
        # with pysftp.Connection(
        #     host=HOSTNAME, 
        #     username=USERNAME, 
        #     password=args.password, 
        #     port=PORT
        # ) as sftp:
        #     return
        #     with sftp.cd('home'):
        #         sftp.put('*.zip')
            
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
