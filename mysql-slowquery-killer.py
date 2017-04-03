#!/usr/bin/env python3
import os
import argparse
import json
import MySQLdb
import MySQLdb.cursors
from datetime import datetime
from time import sleep
from warnings import filterwarnings

filterwarnings('ignore', category=MySQLdb.Warning)

# Threshold time per state
T_TIME_STATISTICS = 20
T_TIME_CREATING_SORT_INDEX = 30


def create_sql(args):

    sql = """
    SELECT * FROM PROCESSLIST
    WHERE COMMAND = 'Query'
    AND (
    (TIME > {0} AND STATE like 'statistics%') OR
    (TIME > {1} AND STATE = 'Creating sort index') OR
    (TIME > {2} AND STATE = 'Sending%') OR
    (TIME > {2} AND STATE = 'Copying%')
    )
    """.format(str(T_TIME_STATISTICS), str(T_TIME_CREATING_SORT_INDEX), str(args.threshold_time))

    # print(sql)
    return sql


def main():
    args = define_parsers()
    db = "INFORMATION_SCHEMA"
    conn = create_db_connection(args.host, args.user, args.passwd, db)

    sql = create_sql(args)

    count = 0

    while True:
        count += 1
        cursor = conn.cursor()
        cursor.execute(sql)

        for row in cursor.fetchall():
            log = query_killer(row, conn, args)
            print(json.dumps(log))

        cursor.close()

        if args.max_count < 0:
            count = args.max_count

        if count > args.max_count:
            break

        sleep(args.interval)

    conn.close()


def query_killer(row, conn, args):
    kill_query = "KILL " + str(row['ID'])

    log = {
        "kill_query": kill_query,
        "timestamp": datetime.now().strftime('%s'),
        "result": "undefined",
        "exec_host": os.uname()[1],
        "mysql_user": args.user,
        "mysql_host": args.host,
        "process_id": row['ID'],
        "process_time": row['TIME'],
        "process_host": row['HOST'],
        "process_user": row['USER'],
        "process_db": row['DB'],
        "process_command": row['COMMAND'],
        "process_state": row['STATE'],
        "process_info": row['INFO'],
    }

    if args.dry_run is True:
        log["result"] = "Dry"

    else:
        # Kill
        try:
            cursor = conn.cursor()
            cursor.execute(kill_query)
            cursor.close()
            log["result"] = "Killed"

        except:
            log["result"] = "Failed"

    return log


def create_db_connection(host, user, password, db):

    connector = MySQLdb.connect(
        host=host,
        user=user,
        passwd=password,
        db=db,
        cursorclass=MySQLdb.cursors.DictCursor,
        use_unicode=True
    )

    return connector


def define_parsers():
    parser = argparse.ArgumentParser(description='Slow query killer for MySQL',
                                     add_help=False)
    parser.add_argument('--help', action='help', help='help')

    parser.add_argument('-u', '--user', type=str, default=os.environ.get('USER', "root"),
                        help='MySQL user. default: USER enviroment value')

    parser.add_argument('-h', '--host', type=str, default=os.environ.get('MYSQL_HOST', "localhost"),
                        help='MySQL host. default: MYSQL_HOST enviroment value')

    parser.add_argument('-p', '--passwd', type=str, default=os.environ.get('MYSQL_PWD', ""),
                        help='MySQL password. default: MYSQL_PWD enviroment value')

    parser.add_argument('--charset', type=str, default="utf8mb4",
                        help='set_client_charset. default: utf8mb4')

    parser.add_argument('-t', '--threshold-time', type=int, default=300,
                        help='Threthold time')

    parser.add_argument('--interval', type=int, default=0,
                        help='Interval time (second)')

    parser.add_argument('--max-count', type=int, default=0,
                        help='Max exec count, -1 is forever')

    parser.add_argument('--dry-run', dest='dry_run', action='store_true',
                        help='Dry run')
    parser.set_defaults(dry_run=False)

    return parser.parse_args()


if __name__ == "__main__":
    main()
