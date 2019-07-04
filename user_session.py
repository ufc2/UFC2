import sqlite3
import uuid
import config
import utils

class user_session(object):

    def __init__(self, path):
        self.conn = sqlite3.connect(path, check_same_thread=False)
        cursor = self.conn.cursor()
        sql = "CREATE TABLE IF NOT EXISTS USER (USERID INT UNIQUE, EMAIL TEXT UNIQUE, PASSWORD TEXT, FIRSTNAME TEXT, LASTNAME TEXT);"
        cursor.execute(sql)
        sql = "CREATE TABLE IF NOT EXISTS SESSION (SESSIONID TEXT UNIQUE, USERID INT);"
        cursor.execute(sql)
        self.conn.commit()

    def __del__(self):
        self.conn.close()

    def register(self, email, password, firstname, lastname):
        sql = "SELECT * FROM USER WHERE EMAIL = '%s'" % email
        ret = self.conn.cursor().execute(sql).fetchall()
        if len(ret) > 0:
            return None
        else:
            user_id = utils.get_new_user_id()
            sql = "INSERT INTO USER (USERID, EMAIL, PASSWORD, FIRSTNAME, LASTNAME) VALUES (%d, '%s', '%s', '%s', '%s')" \
                    % (user_id, email, password, firstname, lastname)
            self.conn.cursor().execute(sql)
            self.conn.commit()
            return { 'user_id': user_id, 'email': email, 'firstname': firstname, 'lastname': lastname }

    def login(self, email, password):
        sql = "SELECT * FROM USER WHERE EMAIL = '%s'" % email
        ret = self.conn.cursor().execute(sql).fetchone()
        dict = {}
        if ret is None:
            dict['code'] = 1
        else:
            if ret[2] != password:
                dict['code'] = 2
            else:
                session_id = str(uuid.uuid1())
                sql = "INSERT INTO SESSION (SESSIONID, USERID) VALUES ('%s', %d)" % (session_id, ret[0])
                self.conn.cursor().execute(sql)
                self.conn.commit()
                dict['code'] = 0
                dict['session_id'] = session_id
                dict['firstname'] = ret[3]
                dict['lastname'] = ret[4]
        return dict

    def logout(self, session_id):
        sql = "DELETE FROM SESSION WHERE SESSIONID = '%s'" % session_id
        self.conn.cursor().execute(sql)
        self.conn.commit()

    def get_user_by_email(self, email):
        sql = "SELECT * FROM USER WHERE EMAIL = '%s'" % email
        ret = self.conn.cursor().execute(sql).fetchone()
        if ret is None:
            return None
        else:
            return { 'user_id': ret[0], 'email': ret[1], 'firstname': ret[3], 'lastname': ret[4] }

    def get_user_by_id(self, user_id):
        sql = "SELECT * FROM USER WHERE USERID = %d" % user_id
        ret = self.conn.cursor().execute(sql).fetchone()
        if ret is None:
            return None
        else:
            return { 'user_id': ret[0], 'email': ret[1], 'firstname': ret[3], 'lastname': ret[4] }

    def get_user_by_session(self, session_id):
        sql = "SELECT * FROM SESSION WHERE SESSIONID = '%s'" % session_id
        ret = self.conn.cursor().execute(sql).fetchone()
        if ret is None:
            return None
        else:
            return self.get_user_by_id(ret[1])

    def get_sessions_by_id(self, user_id):
        sql = "SELECT * FROM SESSION WHERE USERID = '%s'" % user_id
        ret = self.conn.cursor().execute(sql).fetchall()
        sessions = []
        for row in ret:
            sessions.append(row[0])
        return sessions

    def get_sessions_by_email(self, email):
        sql = "SELECT * FROM USER WHERE EMAIL = '%s'" % email
        ret = self.conn.cursor().execute(sql).fetchone()
        if ret is None:
            return []
        return self.get_sessions_by_id(ret[0])
