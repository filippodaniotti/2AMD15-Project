import os
from zipfile import ZipFile

SRC_PATH = os.path.join(os.getcwd(), "src", "2amd15")
ZIP_NAME = os.path.join(os.getcwd(), "app.zip")

def main():
    files = filter(lambda file: os.path.isfile(file), os.listdir(SRC_PATH))
    with ZipFile(ZIP_NAME, "w") as zip:
        for file in files:
            zip.write(os.path.join(SRC_PATH, file))

if __name__ == "__main__":
    main()