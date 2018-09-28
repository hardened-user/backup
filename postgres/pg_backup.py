#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#------------------------------------------------------------------------------
import os
import sys
import re
import datetime
from time import sleep
#
import argparse
import socket
import subprocess
import tempfile


_CNF_GLOBALS_NAME = "globals"
_CNF_DB_EXCLUDE = [_CNF_GLOBALS_NAME, "postgres", "template0", "template1"]


def main():
    global main_return_value
    main_return_value = True
    #______________________________________________________
    # Входящие аргументы
    try:
        parser = argparse.ArgumentParser(description='Postgres Backup', add_help=False)
        parser.add_argument('-h', action='store', type=str, default="", dest="host",
                            metavar='<HOST>', help="database server host or socket directory")
        parser.add_argument('-p', action='store', type=int, default=5432, dest="port",
                            metavar='<PORT>', help="database server port number")
        parser.add_argument('-U', action='store', type=str, default="postgres", dest="user",
                            metavar='<USER>', help="connect as specified database user")
        parser.add_argument('-j', action='store', type=int, default=5, dest="njobs",
                            metavar='<NUM>', help="use this many parallel jobs to dump")
        parser.add_argument('--path', action='store', type=str, required=True, dest="path",
                            metavar='<PATH>', help="backup directory path")
        parser.add_argument('--test', action='store_true', default=False, dest="test",
                            help="test mode")
        parser.add_argument('--help', action='help', help='show this help message and exit')
        args = parser.parse_args()
    except SystemExit:
        return False
    ### path
    if not args.path:
        parser.print_usage()
        return False
    if args.path == ".":
        args.path = os.path.abspath(os.path.dirname(__file__))
    else:
        args.path = os.path.abspath(args.path)
    #______________________________________________________
    # Проверка директорий
    if not check_access_dir('rw', args.path):
        return False
    #______________________________________________________
    # Создание PID файла
    pid_file_path = os.path.join(tempfile.gettempdir(), os.path.basename(sys.argv[0]) + '.pid')
    if not mkpid(pid_file_path):
        print("[EE] Failed to create PID file: '{}'".format(pid_file_path), flush=True)
        return False
    #______________________________________________________
    # Получение версии БД
    pg_db_version = pg_get_version(args.host, args.port, args.user)
    if not pg_db_version:
        pg_db_version = "unknown"
        main_return_value = False
    print("[II] PostgreSQL Server version: '{}'".format(pg_db_version), flush=True)
    #______________________________________________________
    # Получение списка БД
    pg_db_list = pg_get_databases(args.host, args.port, args.user)
    if not pg_db_list:
        print("[EE] Databases not found", flush=True)
        main_return_value = False
    #==============================================================================================
    #==============================================================================================
    # Начало цикла работы
    #==============================================================================================
    # WARNING: Далее не использовать return
    #--------------------------------------------------------------------------
    # Дамп БД
    #--------------------------------------------------------------------------
    for db in pg_db_list:
        tmp_path = os.path.join(args.path, "_tmp_{}_{}".format(db, main_start_dt.strftime("%Y.%m.%d_%H%M%S")))
        dst_path = os.path.join(args.path, main_start_dt.strftime("%Y.%m.%d_%H%M%S"), db)
        print("[..] Dumping database: '{}' ...".format(db), flush=True)
        #__________________________________________________
        if args.test:
            print("[..] Passed (test)", flush=True)
            continue
        #__________________________________________________
        if not pg_dump_database(args.host, args.port, args.user, args.njobs, db, tmp_path):
            print("[EE] Failed to create dump", flush=True)
            main_return_value = False
            continue
        print("[OK] Successfully dumped", flush=True)
        #__________________________________________________
        if not movedump(tmp_path, dst_path):
            main_return_value = False
            continue
    #--------------------------------------------------------------------------
    # Дамп глобальных объектов
    #--------------------------------------------------------------------------
    print("[..] Dumping globals: ...", flush=True)
    tmp_path = os.path.join(args.path, "_tmp_{}_{}".format(_CNF_GLOBALS_NAME, main_start_dt.strftime("%Y.%m.%d_%H%M%S")))
    dst_path = os.path.join(args.path, main_start_dt.strftime("%Y.%m.%d_%H%M%S"), _CNF_GLOBALS_NAME)
    if args.test:
        print("[..] Passed (test)", flush=True)
    else:
        if not pg_dump_globals(args.host, args.port, args.user, tmp_path):
            print("[EE] Failed to create dump", flush=True)
            main_return_value = False
        else:
            print("[OK] Successfully dumped", flush=True)
            if not movedump(tmp_path, dst_path):
                main_return_value = False
    #==============================================================================================
    # Удаление PID файла
    if not rmfile(pid_file_path):
        print("[EE] Failed to delete PID file: '{}'".format(pid_file_path), flush=True)
        main_return_value = False
    #______________________________________________________
    return main_return_value

#==================================================================================================
# Functions
#==================================================================================================
def mkpid(path):
    '''Создание PID файла. Без ожидания'''
    #______________________________________________________
    if os.path.exists(path):
        try:
            f = open(path, 'r')
            pid = f.readline().strip()
            f.close()
        except Exception as err:
            print("[!!] Exception Err: {}".format(err), flush=True)
            print("[!!] Exception Inf: {}".format(sys.exc_info()), flush=True)
            return False
        if pid.isdigit():
            #______________________________________________
            # Проверка состояния процесса по PID'у
            try:
                os.kill(int(pid), 0)
            except OSError:
                print("[WW] Pid file alredy exist, but process with PID:{} does not exist".format(pid), flush=True)
            else:
                print("[EE] Pid file alredy exist, but process with PID:{} is launched".format(pid), flush=True)
                return False
        else:
            #______________________________________________
            # Ошибка содержимого файла
            print("[EE] Pid file alredy exist, but PID incorrect. Remove file manually: '{}'".format(path), flush=True)
            return False
    #______________________________________________________
    try:
        f = open(path, 'w')
        f.write('{}\n'.format(os.getpid()))
        f.close()
    except Exception as err:
        print("[!!] Exception Err: {}".format(err), flush=True)
        print("[!!] Exception Inf: {}".format(sys.exc_info()), flush=True)
        return False
    #______________________________________________________
    return True


def rmfile(path):
    try:
        os.remove(path)
    except Exception as err:
        print("[!!] Exception Err: {}".format(err), flush=True)
        print("[!!] Exception Inf: {}".format(sys.exc_info()), flush=True)
        return False
    #______________________________________________________
    return True


def movedump(src, dst):
    '''Перемещение файла/каталога'''
    d = os.path.dirname(dst)
    if os.path.exists(d):
        if not os.path.isdir(d):
            print("[EE] Is not directory: '{}'".format(d), flush=True)
            return False
    else:
        try:
            os.mkdir(d)
        except Exception as err:
            print("[!!] Exception Err: {}".format(err), flush=True)
            print("[!!] Exception Inf: {}".format(sys.exc_info()), flush=True)
            print("[EE] Failed to create directory: '{}'".format(d), flush=True)
            return False
    #______________________________________________________
    try:
        os.rename(src, dst)
    except Exception as err:
        print("[!!] Exception Err: {}".format(err), flush=True)
        print("[!!] Exception Inf: {}".format(sys.exc_info()), flush=True)
        return False
    #______________________________________________________
    return True


def check_access_dir(mode, *args):
    return_value = True
    modes_dict = {'ro': os.R_OK, 'rx': os.X_OK, 'rw': os.W_OK}
    for x in args:
        if not x:
            print("[EE] Directory is not specified", flush=True)
            return_value = False
            continue
        if os.path.exists(x):
            if os.path.isdir(x):
                if not os.access(x, modes_dict[mode]):
                    print("[EE] Access denied: '{}' ({})".format(x, mode), flush=True)
                    return_value = False
            else:
                print("[EE] Is not directory: '{}'".format(x), flush=True)
                return_value = False
        else:
            print("[EE] Does not exist: '{}'".format(x), flush=True)
            return_value = False
    #______________________________________________________
    return return_value


def shell_exec(cmd, stdin=None):
    cmd = '''export LC_ALL="C"; export LANG="en_US.UTF-8"; {}'''.format(cmd)
    child = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, stdin=subprocess.PIPE)
    stdout = child.communicate(input=stdin)[0]
    returncode = child.returncode
    #______________________________________________________
    return (returncode, stdout)


def pg_get_version(host, port, user):
    '''Получение версии БД'''
    #______________________________________________________
    cmd = '''psql -h "{}" -p {} -U "{}" -tA -c "SHOW server_version;"'''.format(host, port, user)
    rc, rd = shell_exec(cmd)
    if rc != 0 or not rd:
        print("[EE] Shell exec rc:{}".format(rc), flush=True)
        print(cmd, flush=True)
        print("-{}".format("  -"*33), flush=True)
        print(rd.decode('utf-8'), flush=True)
        print("-{}".format("  -"*33), flush=True)
        return None
    #______________________________________________________
    return rd.decode('utf-8').strip()


def pg_get_databases(host, port, user):
    '''Получение списка БД'''
    re_db_name = re.compile('^\s*([\w]+)\s*\|')
    #______________________________________________________
    cmd = '''psql -h "{}" -p {} -U "{}" -t -l'''.format(host, port, user)
    rc, rd = shell_exec(cmd)
    if rc != 0 or not rd:
        print("[EE] Shell exec rc:{}".format(rc), flush=True)
        print(cmd, flush=True)
        print("-{}".format("  -"*33), flush=True)
        print(rd.decode('utf-8'), flush=True)
        print("-{}".format("  -"*33), flush=True)
        return list()
    #______________________________________________________
    _tmp = rd.decode('utf-8').split('\n')
    _tmp = filter(lambda x: re_db_name.search(x), _tmp)
    _tmp = map(lambda x: re_db_name.search(x).group(1), _tmp)
    _tmp = filter(lambda x: x not in (_CNF_DB_EXCLUDE), _tmp)
    #______________________________________________________
    return list(_tmp)


def pg_dump_database(host, port, user, njobs, db, f):
    '''Создание дампа БД'''
    cmd = '''pg_dump -h "{}" -p {} -U "{}" -j {} -Fd "{}" -f "{}"'''.format(host, port, user, njobs, db, f)
    rc, rd = shell_exec(cmd)
    if rc != 0:
        print("[EE] Shell exec rc:{}".format(rc), flush=True)
        print(cmd, flush=True)
        print("-{}".format("  -"*33), flush=True)
        print(rd.decode('utf-8'), flush=True)
        print("-{}".format("  -"*33), flush=True)
        return False
    #______________________________________________________
    return True


def pg_dump_globals(host, port, user, f):
    '''Создание дампа глобальных объектов'''
    cmd = '''pg_dumpall -h "{}" -p {} -U "{}" --globals-only -f "{}"'''.format(host, port, user, f)
    rc, rd = shell_exec(cmd)
    if rc != 0:
        print("[EE] Shell exec rc:{}".format(rc), flush=True)
        print(cmd, flush=True)
        print("-{}".format("  -"*33), flush=True)
        print(rd.decode('utf-8'), flush=True)
        print("-{}".format("  -"*33), flush=True)
        return False
    #______________________________________________________
    return True


#%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
main_start_dt = datetime.datetime.now()
if __name__ == '__main__':
    print("-"*100, flush=True)
    print("{} HOST:{} PID:{} PPID:{} NAME:'{}'".format(main_start_dt, socket.getfqdn(), os.getpid(), os.getppid(), os.path.basename(sys.argv[0])), flush=True)
    print("-"*100, flush=True)
    #______________________________________________________
    exit_value = main()
    #______________________________________________________
    print("-"*100, flush=True)
    print("{} PID:{} TIME:{} RETURN:{}".format(datetime.datetime.now(), os.getpid(), datetime.datetime.now() - main_start_dt, exit_value), flush=True)
    print("-"*100 + '\n', flush=True)
    #______________________________________________________
    sys.exit(not exit_value) # BASH compatible
