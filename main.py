#!/usr/bin/python3
# -*- coding: utf-8 -*-

from argparse import ArgumentParser
import os
import sqlite3
import subprocess
import time


class LockMultipleLaunch:
    def __init__(self, lock_file):
        self.lock_file = lock_file
        self.lock = False
        self.set_lock()

    def set_lock(self):
        while not self.lock:
            if not os.path.exists(self.lock_file):
                self.__lf = open(self.lock_file, "w")
                self.lock = True
            else:
                try:
                    os.remove(self.lock_file)
                except:
                    self.lock = False
                    return

    def set_unlock(self):
        if self.lock:
            self.lock = False
            self.__lf.close()
            os.remove(self.lock_file)

    def __del__(self):
        self.set_unlock()


class ConsoleReport:
    def __init__(self):
        self.__table = []  # Таблица для временного хранения отчёта
        self.__cell_length = []  # Список для хранения количества символов в полях таблицы
        self.__min_row_length = 3  # Минимальная длина ячейки в таблице. Используется при печати с переносом строки.

    def append(self, lst):
        """
        Функция для добавления в отчёт строки с данными
        :param lst: Список с данными для отчёта
        :return: 
        """
        # Если таблица уже существует и количество ячеек добавляемой строки не соответсвует количеству полей таблицы,
        # вызываем исключение
        if self.__table:
            if len(self.__table[0]) != len(lst):
                raise NameError("The length of the added list no longer corresponds to the entered.")
        # Если список длинн полей таблицы пуст, заполняем его нулями по количеству полей в переданной строке таблицы
        if not self.__cell_length:
            for _ in range(len(lst)):
                self.__cell_length.append(0)
        # Перебираем поля в переданной строке таблицы
        for _ in range(len(lst)):
            ln = len(str(lst[_]))
            # Если длина текущего поля в переданной строке таблицы больше больше длины ранее переданных полей,
            # увеличиваем хранимое значение до текущего
            if ln > self.__cell_length[_]:
                self.__cell_length[_] = ln
        self.__table.append(lst)

    def __print_line(self, line, length_list, border=True):
        """
        Печать строки таблицы в соответствии с переданными длинами ячеек
        :param : Список с ячейками таблицы
        :param length_list: Список с длинами ячеек таблицы
        :param border: Печатать или нет рамку вокруг ячеек таблицы
        :return: 
        """
        for col in range(len(line)):
            if border:
                print("| ", end="")
            else:
                print(" ", end="")
            print(line[col], end="")
            print(" " * (length_list[col] - len(str(line[col]))), end="")
            print(" ", end="")
        if border:
            print("|", end="")
        print()

    def __print_border(self, length_list, border=True):
        """
        Печать межстрочного разделителя
        :param length_list: Список с длинами ячеек таблицы
        :param border: Печатать или нет рамку вокруг ячеек таблицы
        :return: 
        """
        if border:
            print("+", end="")
            for _ in length_list:
                print("-" * (_ + 2), end="")
                print("+", end="")
            print()

    def print_wrapped(self, max_length, border=True):
        """
        Вывод отчёта на экран с переносом строк в ячейках
        :param max_length: Максимальная допустимая длина строки терминала
        :param border: Печатать или нет рамку вокруг ячеек таблицы
        :return: 
        """
        # Вычисляем общую длинну строки в существующей таблице
        border_length = (4 + (len(self.__cell_length) - 1) * 3) if border else len(self.__cell_length) * 2
        current_summ_length = sum(self.__cell_length)
        new_summ_length = max_length - border_length
        if max_length and max_length < current_summ_length + border_length:
            new_cell_length = []
            # Наполняем список длин новыми значениями
            for l in self.__cell_length:
                nl = int(new_summ_length/(current_summ_length/l))
                new_cell_length.append(nl if nl > self.__min_row_length else self.__min_row_length)
            # Делаем копию таблицы для того, что бы не повредить данные в отчёте
            table = []
            for line in self.__table:
                table.append(line.copy())
            # Печатаем таблицу в соответсвии с новой длиной полей
            for line in table:
                # Верхнее обрамление строки
                self.__print_border(new_cell_length, border)
                # Строка таблицы
                wrap = True  # Признак переноса строки
                while wrap:
                    wrap = False
                    new_line = []
                    for r in range(len(line)):
                        if type(line[r]) != str:
                            line[r] = str(line[r])
                        if line[r] and line[r][0] == " ":
                            line[r] = line[r][1:]
                        new_line.append(line[r][:new_cell_length[r]])
                        line[r] = line[r][new_cell_length[r]:]
                        if line[r]:
                            wrap = True
                    self.__print_line(new_line, new_cell_length, border)
            # Нижнее обрамление последней строки
            self.__print_border(new_cell_length, border)
        print()

    def print(self, border=True):
        """
        Вывод отчёта на экран
        :param border: Печатать или нет рамку вокруг ячеек таблицы
        :return: 
        """
        for line in self.__table:
            # Верхнее обрамление строки
            self.__print_border(self.__cell_length, border)
            # Строка таблицы
            self.__print_line(line, self.__cell_length, border)
        # Нижнее обрамление последней строки
        self.__print_border(self.__cell_length, border)
        print()


def main(edit_schedule):
    db_con = sqlite3.connect("sheduler.sl3")
    # Если таблицы нет, создаём её
    query = """
        CREATE TABLE if not exists main (
            id            INTEGER NOT NULL
                                  PRIMARY KEY AUTOINCREMENT,
            job_name              NOT NULL,
            path                  NOT NULL,
            minute                NOT NULL,
            hour                  NOT NULL,
            day_of_month          NOT NULL,
            month_of_year         NOT NULL,
            day_of_week           NOT NULL
        );
         """
    for q in query.split(";"):
        db_con.execute(q)

    # Если был произведён запуск программы с ключом -е, переходим в режим позволяющий редактировать список задач
    if edit_schedule:
        # Выводим список доступных действий
        cmd_help = "\nВведите одну из следующих команд:\n" \
                   "\ta - Добавить задачу\n" \
                   "\td - Удалить задачу\n" \
                   "\tl - Вывести список задач\n" \
                   "\tq - Выход\n"

        cmd_quit = False
        while not cmd_quit:
            print(cmd_help)
            cmd_line = input(":")
            # Выход из программы
            if cmd_line == "q":
                cmd_quit = True
            # Вывод списка задач и базы
            elif cmd_line == "l":
                cr = ConsoleReport()
                cr.append(["ID", "MIN", "HOUR", "DOM", "MOY", "DOW", "PATH"])
                query = "select id, minute, hour, day_of_month, month_of_year, day_of_week, path from main"
                for task in db_con.execute(query).fetchall():
                    cr.append(list(task))
                cr.print_wrapped(76)
            # Добавление новой задачи
            elif cmd_line == "a":
                print("Введите имя задачи")
                job_name = input("::")
                print("Введите путь к исполняемуому файлу или текст скрипта одной строкой")
                job_path = input("::")
                print("Введите временные интервалы выполнения задачи в формате CRONTAB разделённые пробелами\n"
                      "МИНУТЫ   ЧАСЫ   ДЕНЬ_МЕСЯЦА   МЕСЯЦ_ГОДА   НОМЕР_ДНЯ_НЕДЕЛИ")
                job_time = input("::")
                job_time_list = job_time.split(" ")
                if type(job_time_list) == list and len(job_time_list) == 5:
                    query = "insert into main (job_name, path, minute, hour, day_of_month, month_of_year, day_of_week)" \
                            "values('%s', '%s', '%s', '%s', '%s', '%s', '%s')" % (job_name,
                                                                                  job_path,
                                                                                  job_time_list[0],
                                                                                  job_time_list[1],
                                                                                  job_time_list[2],
                                                                                  job_time_list[3],
                                                                                  job_time_list[4])
                    try:
                        db_con.execute(query)
                        db_con.commit()
                        print("Задача добавлена")
                    except:
                        print("ОШИБКА: Задача не была добавлена")
                else:
                    print("ОШИБКА: Не правильно указан временной интревал")
            # Удаление задачи по её ID
            elif cmd_line == "d":
                print("Введите ID задачи, которую нужно удалить")
                job_id = input("::")
                if type(job_id) == str and job_id == "q":
                    continue
                query = "select job_name from main where id = %s" % job_id
                result = db_con.execute(query).fetchall()
                if len(result):
                    job_name = result[0]
                    query = "delete from main where id = %s" % job_id
                    try:
                        db_con.execute(query)
                        db_con.commit()
                        print("Задача '%s' удалена" % job_name)
                    except:
                        print("ОШИБКА: Не могу удалить задучу ID %s" % job_id)
                else:
                    print("ОШИБКА: Не правильный ID")
        return

    # Проверяем не запущен ли другой экземпляр этой программы в настоящее время,
    # чтобы предотвратить неоднократное выполнение запланированных задач в одно и тоже время
    lml = LockMultipleLaunch(os.path.splitext(__file__)[0] + ".lock")
    if lml.lock:
        while True:
            cur_time = time.localtime(time.time())

            # Выбираем из базы все задачи
            query = "select * from main"
            job_list = db_con.execute(query).fetchall()
            # Проверяем каждую задачу на соответствие текущему моменту времени
            for job in job_list:
                # Список для хранения представления текущего времени
                cur_time_list = [cur_time.tm_min,
                                 cur_time.tm_hour,
                                 cur_time.tm_mday,
                                 cur_time.tm_mon,
                                 cur_time.tm_wday+1]
                # Список в котором хранится время выбранной задачи в формате cron.
                # Дальше будем приводить элементы этого списка к формату текущего времени или оставлять как есть,
                # если это будет не возможно.
                job_time_list = list(job[2:])
                # Перебираем элементы списков
                for _ in range(5):
                    # Если текущее значение списка job_time_list является строкой
                    if type(job_time_list[_]) == str:
                        # Если текущее значение списка job_time_list подрузамевает повторяющиеся значения
                        if "*/" in job_time_list[_]:
                            # Из текущего значения списка job_time_list ыделяем делитель
                            divider = int(job_time_list[_].replace("*/", ""))
                            # Вычисляем остаток от деления элемента списка текущего времени на делитель
                            modulo = cur_time_list[_] % divider
                            # Если остаток от деления равен нулю, текущий элемент списка job_time_list
                            # делаем равным элементу списка текущего времени
                            if modulo == 0:
                                job_time_list[_] = cur_time_list[_]
                        # Если текущий элемент списка в формате cron является "любым значением",
                        # присваиваем этому элементу значение из списка текущего времени
                        elif job_time_list[_] == "*":
                            job_time_list[_] = cur_time_list[_]
                        # Если в элементе списка job_time_list перечисляются числовые значения
                        elif "," in job_time_list[_]:
                            # Создаём переменную для хранения числовых значений
                            values = job_time_list[_].split(",")
                            # Перебираем эти значения и сравниваем их со значением из списка текущего времени
                            for value in values:
                                # Если значения совпадают, присваиваем элементу списка job_time_list
                                # значение из списка текущего времени
                                if int(value) == cur_time_list[_]:
                                    job_time_list[_] = cur_time_list[_]
                        # Если в элементе списка job_time_list описывается временной интервал
                        elif "-" in job_time_list[_]:
                            start_time, stop_time = job_time_list[_].split("-")
                            if int(start_time) <= cur_time_list[_] <= int(stop_time):
                                job_time_list[_] = cur_time_list[_]
                # Если список текущего времени идентичен списку времени выполнения задачи из БД, запускаем задачу
                if job_time_list == cur_time_list:
                    job_name, job_path = job[:2]
                    if os.path.exists(job_path):
                        home_dir = os.path.dirname(job_path)
                    else:
                        home_dir = os.path.dirname(__file__)
                    try:
                        pid = subprocess.Popen(job_path,
                                               shell=True,
                                               cwd=home_dir,
                                               stdout=subprocess.PIPE,
                                               ).pid
                        print("%s\tЗадача '%s' запущена. PID = '%s'" % (time.strftime("%Y-%m-%d %H:%M:%S",
                                                                       time.localtime(time.time())),
                                                                    job_name,
                                                                    pid))
                    except:
                        print("ОШИБКА: Задача '%s' не запустилась." % job_name)

            if cur_time.tm_sec > 1:
                second = 61 - cur_time.tm_sec
            else:
                second = 60
            time.sleep(second)
    # Если другой экземпляр этой программы уже запущен, прекращаем выполнение программы
    else:
        print("Один экземпляр программы уже запущен. Не возможно запустить второй экземпляр программы.")


if __name__ == "__main__":
    parser = ArgumentParser()
    parser.add_argument('-e', '--edit', action='store_true', help='Редактирование списка задач.')
    args = parser.parse_args()

    main(args.edit)
