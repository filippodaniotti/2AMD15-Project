import os
import re
import logging

import pysftp
from subprocess import run
from getpass import getpass
from zipfile import ZipFile
from contextlib import contextmanager
from argparse import ArgumentParser, Namespace

from typing import List, Dict, Set, Union


logging.basicConfig(format = '%(levelname)s: %(message)s')
logger = logging.getLogger(__name__)

@contextmanager
def ondir(path: str):
    cwd = os.getcwd()
    dest = os.path.join(cwd, path)
    os.chdir(dest)
    yield dest
    os.chdir(cwd)

SRC_PATH = os.path.join("src", "2amd15")
JAR_PATH = os.path.join("tools", "GenVec", "GenVec.jar")

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
            if re.match(r"^(\s)*q[2-4]", line) and f"q{question}" not in line:
                line = "#" + line
            f.write(line)

def generate_source_archive(question: int):    
    file_names = [f"question{question}.py", MAIN_NAME]
    
    imports = ImportsParser()
    code_lines: Dict[str, List[str]] = {}
    
    logger.debug("Parsing source code")
    with ondir(SRC_PATH):
        if (file_names[0] not in os.listdir() or
            file_names[1] not in os.listdir()):
            raise RuntimeError("Missing files in source directory")
        for file in file_names:
            logger.debug(f"{file} found, analysing...")
            imports.update_imports(file)
            lines = extract_code_lines(file)
            lines = parse_configuration(lines)
            code_lines[file] = lines
     
    logger.info(f"Building {MAIN_NAME}...")
    build_main(question, code_lines, file_names, imports)
    with ZipFile(SRC_ZIP_NAME, "w") as zip:
        logger.debug(f"Compressing into {SRC_ZIP_NAME}")
        zip.write(MAIN_NAME)
    if os.path.isfile(MAIN_NAME):
        os.remove(MAIN_NAME)
    logger.info("Done")
                    
def generate_data_archive(question: int, rows: Union[int, None], cols: Union[int, None]):
    rows = rows if rows is not None else NUMBER_OF_VECTORS[question]
    cols = cols if cols is not None else NUMBER_OF_COLUMNS
    logger.info(f"Generating new {VECTORS_NAME} file with\n\t- {cols} columns\n\t- {rows} rows")
    with ZipFile(DATA_ZIP_NAME, "w") as zip:
        run([
            "java", 
            "-jar", 
            JAR_PATH, 
            str(GROUP_NUMBER), 
            str(rows), 
            str(cols)
        ])
        logger.debug(f"Compressing into {DATA_ZIP_NAME}...")
        zip.write(VECTORS_NAME)
    logger.info("Done")

def main(args: Namespace):
    logger.info(f"Calling {__file__} on question {args.question}")
    generate_data_archive(args.question, args.rows, args.cols)
    generate_source_archive(args.question)
    if args.submit:
        logger.info(f"-s was passed, deploying to cluster")
        with pysftp.Connection(
            host=HOSTNAME, 
            username=USERNAME, 
            password=args.password, 
            port=PORT,
            auto_add_key=True
        ) as sftp:
            with sftp.cd('home'):
                sftp.put(SRC_ZIP_NAME)
                sftp.put(DATA_ZIP_NAME)
            logger.info("Submission complete")
            
if __name__ == "__main__":
    parser = ArgumentParser(description="Handles the building pipeline of the submission artifact.")
    parser.add_argument(
        "-s",
        "--submit",
        action="store_true",
        dest="submit",
        required=False,
        help="Proceed with creating a submission on the server after build"
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
        help="password of the server, required if -s is passed",
    )
    parser.add_argument(
        "-v",
        "--verbose",
        dest="verbose",
        action="store_true",
        required=False,
        help="verbose flag, sets logging level to debug",
    )
    parser.add_argument(
        "-r",
        "--rows",
        dest="rows",
        type=int,
        required=False,
        help="number of vectors in the csv, overwrites default",
    )
    parser.add_argument(
        "-c",
        "--columns",
        dest="cols",
        type=int,
        required=False,
        help="length of vectors in the csv, overwrites default",
    )
    args = parser.parse_args()
    logger.setLevel(logging.DEBUG if args.verbose else logging.INFO)
    if (args.submit and not args.password):
        args.password = getpass()
    main(args)
