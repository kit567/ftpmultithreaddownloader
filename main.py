#!/usr/bin/python
# -*- coding: utf-8
import DownloadMultiTreading_config as config
import ftplib
import threading

class BaseFileDownload(threading.Thread):
    """ Объект для копируемого файла """
    count = 0

    def __init__(self, rpath, filename, log):
        threading.Thread.__init__(self)
        self.remote_path = rpath
        self.filename = filename
        self.ftp = None
        self.command = None
        self.currentpath = None
        self.log = log
        self.__class__.count += 1 # для подсчета одновременно запущенных закачек

    def __del__(self):
        self.__class__.count -= 1

    def connect(self):
        """Метод для соединения с ftp"""
        import sys
        self.ftp = MyFtp()
        self.ftp.connect()
        self.ftp.login()
        self.ftp.__class__.encoding = sys.getfilesystemencoding()


    def run(self):
        """Запуск потока скачивания"""
        import os
        self.connect()
        self.command = str(bytes('RETR ', encoding='latin-1'), encoding='utf-8')
        self.currentpath = os.path.join(basepath, self.remote_path[3:])
        self.ftp.cwd(self.remote_path)
        if not os.path.exists(self.currentpath):
            os.makedirs(self.currentpath, exist_ok=True)
        self.host_file = os.path.join(self.currentpath, self.filename)
        try:
            with open(self.host_file, 'wb') as local_file:
                self.log.add("Start downloading " + self.filename)
                self.ftp.retrbinary(self.command + self.filename, local_file.write)
                self.log.add("Downloading " + self.filename + " complete")
        except ftplib.error_perm:
            self.log.add_error('Permission error')
        self.ftp.quit()


class MyLogger:
    """Класс для логирования событий"""
    def __init__(self):
        self.logger = None

    def start_file_logging(self, logger_name, log_path):
        """Обычное логирование в файл"""
        import logging
        self.logger = logging.getLogger(logger_name)
        self.logger.setLevel(logging.INFO)
        try:
            fh = logging.FileHandler(log_path)
        except FileNotFoundError:
            log_path = "downloader.log"
            fh = logging.FileHandler(log_path)
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        fh.setFormatter(formatter)
        self.logger.addHandler(fh)

    def start_rotate_logging(self, logger_name, log_path, max_bytes=104857600, story_backup=5):
        """Логирование в файл с ротацией логов"""
        import logging
        from logging.handlers import RotatingFileHandler
        self.logger = logging.getLogger(logger_name)
        self.logger.setLevel(logging.INFO)
        try:
            fh = RotatingFileHandler(log_path, maxBytes=max_bytes, backupCount=story_backup)
        except FileNotFoundError:
            log_path = "downloader.log"
            fh = RotatingFileHandler(log_path, maxBytes=max_bytes, backupCount=story_backup)
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        fh.setFormatter(formatter)
        self.logger.addHandler(fh)

    def add(self, msg):
        self.logger.info(str(msg))

    def add_error(self, msg):
        self.logger.error(str(msg))


class FileList:
    """Класс для работы со списком загружаемых файлов"""
    def __init__(self):
        self.ftp = None
        self.file_list = []

    def connect_ftp(self):
        import sys
        self.ftp = MyFtp()
        self.ftp.connect()
        self.ftp.login()
        self.ftp.__class__.encoding = sys.getfilesystemencoding()

    def save_list(self):
        import json
        with open("files_hash.json", 'w') as f:
            json.dump(self.file_list, f, indent=2)

    def get_list(self, name):
        """Метод для получения списка всех файлов с ftp-сервера."""
        import os
        for dirname in self.ftp.mlsd(str(name), facts=["type"]):
            if dirname[1]["type"] == "file":
                entry_file_list = {}
                entry_file_list['remote_path'] = name  #путь до файла
                entry_file_list['filename'] = dirname[0]  #имя файла
                self.file_list.append(entry_file_list)
            else:
                path = os.path.join(name, dirname[0])
                self.get_list(path)

    def get_next_file(self):
        return self.file_list.pop()

    def len(self):
        return len(self.file_list)


class MyFtp (ftplib.FTP):
    """Класс переопределяет стандартный, чтобы задать все параметры соединение в одном месте"""
    def __init__(self):
        self.host = host
        self.user = user
        self.passwd = passwd
        self.timeout = 1800
        super(MyFtp, self).__init__()

    def connect(self):
        super(MyFtp, self).connect(self.host, timeout=self.timeout)

    def login(self):
        super(MyFtp, self).login(user=self.user, passwd=self.passwd)

    def quit(self):
        super(MyFtp,self).quit()


class StatusFile:
    """По окончанию задачи скрипт пишет в файл уведомление о корректном выполнении."""
    def __init__(self):
        self.msg = ''

    def setstatus(self, msg):
        global statusfilepath
        with open(statusfilepath, 'w') as status_file:
            status_file.write(msg)


def main():
    import os
    import datetime
    import time

    log = MyLogger()
    log.start_rotate_logging("DownloaderLog", os.path.join(log_path, "download_backup.log"))
    now = datetime.datetime.today().strftime("%Y%m%d")
    global basepath
    basepath = os.path.join(basepath, now)  # модифицируем путь, добавляя текущую дату
    list_file = FileList()
    list_file.connect_ftp()
    list_file.get_list("..")
    for i in range(list_file.len()):
        flag = True
        while flag:   # цикл внутри которого поддреживает нужное количество одновременно запущенных загрузок
            if BaseFileDownload.count < max_threads:
                curfile = list_file.get_next_file()
                threadid = BaseFileDownload(curfile["remote_path"], curfile["filename"], log)
                threadid.start()
                flag = False
            else:
                time.sleep(20)
    log.add("Downloading files complete")
    statusfile = StatusFile()
    statusfile.setstatus("Downloading at " + str(datetime.datetime.now()) + " finishing successful")

if __name__ == "__main__":
    host = config.host
    user = config.user
    passwd = config.passwd
    basepath = config.basepath  # Папка, в которой будут созданы подпапки со скачанными файлами
    max_threads = config.max_threads
    log_path = config.log_path
    statusfilepath = config.statusfilepath
    main()
