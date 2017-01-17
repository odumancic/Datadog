#!/usr/bin/env python
# coding: utf-8

from checks import AgentCheck
import pymysql
import psycopg2


class flydata(AgentCheck):

    def check(self, instance):
        server = instance[u'server']
        user = instance[u'user']
        passwd = instance[u'pass']
        db = instance[u'db']
        port = instance[u'port']
        type = instance[u'type']
        schema = instance[u'schema']
        if type == 'mysql':
            try:
                mysql = Connect(server, user, passwd, db, port, schema)
                a = mysql.connect_mysql()
                b = mysql.measure_mysql(a)
                for i in b:
                    self.gauge('flydata.bidb.mysql.count', b[i], [
                               u'table:{}'.format(str([i]).strip('[]\''))])
            except Exception as detail:
                print ('Connection failed:', detail)
        else:
            try:
                redshift = Connect(server, user, passwd, db, port, schema)
                pg1 = redshift.connect_redshift()
                pg = redshift.measure_redshift(pg1)
                for i in pg:
                    self.gauge('flydata.bidb.redshift.count', pg[i], [
                               u'table:{}'.format(str([i]).strip('[]\''))])
            except Exception as detail:
                print ('Connection failed:', detail)


class Connect(object):

    def __init__(self, server, user, passwd, db, port, schema):
        self.table_name = []
        self.server = server
        self.user = user
        self.passwd = passwd
        self.db = db
        self.port = port
        self.schema = schema

    def connect_mysql(self):
        db = pymysql.connect(self.server, self.user,
                             self.passwd, self.db, self.port)
        return db

    def connect_redshift(self):
        conn_string = "host='%s' dbname='%s' user='%s'" \
                "password='%s' port='%s'" % (
                 self.server, self.db, self.user, self.passwd, self.port)
        db = psycopg2.connect(conn_string)
        return db

    def measure_mysql(self, db):
        cursor = db.cursor()
        cursor.execute('SHOW TABLES')
        self.cursor = cursor
        tmp = {}
        for table_name in self.cursor:
            sql = """select count(*) from %s""" % (" | ".join(table_name))
            cursor1 = db.cursor()
            cursor1.execute(sql)
            requestcount = cursor1.fetchone()[0]
            for i in table_name:
                tmp[i] = " | ".join(table_name)
                tmp[i] = requestcount
            cursor1.close()
        cursor.close()
        return tmp

    def measure_redshift(self, db):
        cursor = db.cursor()
        get_tables = """select relname from pg_stat_user_tables" \
                     "where schemaname = '%s' """ % (
            self.schema)
        cursor.execute(get_tables)
        self.cursor = cursor
        tmp = {}
        for table_name in self.cursor:
            sql = """select count(*) from %s.%s""" % (self.schema,
                                                      " | ".join(table_name))
            cursor1 = db.cursor()
            cursor1.execute(sql)
            requestcount = cursor1.fetchone()[0]
            for i in table_name:
                tmp[i] = " | ".join(table_name)
                tmp[i] = requestcount
            cursor1.close()
        cursor.close()
        return tmp


if __name__ == '__main__':
    instances = flydata.from_yaml('/etc/dd-agent/conf.d/flydata.yaml')
    for instance in instances:
        print "\nRunning the check against url: %s" % (instance['db'])
