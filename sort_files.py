import argparse
from threading import Thread
from concurrent.futures import ThreadPoolExecutor
from multiprocessing import cpu_count
from pathlib import Path
from shutil import rmtree
import logging

RENAME_PATTERN = "_renamed_{:0>3}_"


class ThreadList:
    @classmethod
    def add(cls, thread: Thread):
        if not hasattr(cls, "threads"):
            cls.threads = []
        cls.threads.append(thread)

    @classmethod
    def join_all(cls):
        if hasattr(cls, "threads"):
            for thread in cls.threads:
                thread.join()


class Files:
    @classmethod
    def add(cls, file_path: Path):
        if not hasattr(cls, "files"):
            cls.files = []
        cls.files.append(file_path)

    @classmethod
    def get_list(cls):
        return cls.files if hasattr(cls, "files") else []


class ProcessFile:
    locked_names = set()

    def __init__(self, path: Path):
        self.path = path

    def __call__(self, file_path: Path):
        self.lock_file_name(file_path.name.lower())
        folder = file_path.suffix
        logging.debug(f"{folder} / {file_path.name}")
        self.prepare_target_folder(folder)
        new_file = self.get_unique_path(self.path / folder / file_path.name)
        file_path.replace(new_file)
        self.unlock_file_name(file_path.name.lower())

    def lock_file_name(self, file_name: str):
        while file_name in ProcessFile.locked_names:
            pass
        ProcessFile.locked_names.add(file_name)

    def unlock_file_name(self, file_name: str):
        ProcessFile.locked_names.remove(file_name)

    def get_unique_path(self, file_path: Path) -> Path:
        new_file = file_path
        if new_file.exists():
            attempt = 0
            new_name = file_path.stem + RENAME_PATTERN + file_path.suffix
            while True:
                attempt += 1
                new_file = file_path.parent / new_name.format(attempt)
                if not new_file.exists():
                    break
        return new_file

    def prepare_target_folder(self, folder_name: str):
        target_folder = self.path / folder_name
        if target_folder.is_file():
            tmp_file = self.get_unique_path(target_folder)
            target_folder.replace(tmp_file)
            target_folder.mkdir()
            tmp_file.replace(target_folder / folder_name)
        else:
            target_folder.mkdir(exist_ok=True, parents=True)


def process_folder(folder: Path):
    logging.debug(f"Folder '{folder.stem + folder.suffix}'")
    for f in folder.iterdir():
        if f.is_dir():
            new_thread = Thread(target=process_folder, args=(f,))
            ThreadList().add(new_thread)
            new_thread.start()
        else:
            Files().add(f)

def check_source_folder(source_folder: Path):
    if not source_folder.exists():
        exit(f'ERROR: The specified path ({source_folder}) does not exist.')
    if not source_folder.is_dir():
        exit(f'ERROR: The specified path ({source_folder}) is not a folder.')
    try:
        output_folder.mkdir(parents=True, exist_ok=True)
    except Exception as e:
        exit(e)


if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG, format='%(threadName)s %(message)s')
    parser = argparse.ArgumentParser(description='Sorting folder')
    parser.add_argument('--source', '-s', required=True, help='Source folder')
    parser.add_argument('--output', '-o', default='sorted', help='Output folder')
    args = vars(parser.parse_args())
    source_folder = Path(args.get('source'))
    output_folder = Path(args.get('output'))
    check_source_folder(source_folder)
    logging.debug(f"Processing folder '{source_folder.resolve()}'...")
    process_folder(source_folder)
    ThreadList().join_all()
    logging.debug(f"Total number of files: {len(Files().get_list())}")
    number_of_threads = cpu_count() + 4
    process_file = ProcessFile(output_folder)
    with ThreadPoolExecutor(number_of_threads) as pool:
        _ = pool.map(process_file, Files().get_list())
    rmtree(source_folder)
