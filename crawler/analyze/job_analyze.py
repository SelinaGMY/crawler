import logging
import json
import datetime


class JobAnalyze:
    def __init__(self, company, source):
        self.source = source
        self.company = company

        self.total_nu = 0  # 当天职位总数量
        self.add_nu = 0   # 当天相比昨天增加数量
        self.delete_nu = 0  # 当天相比昨天删除数量
        self.update_nu = 0  # 当天相比昨天更新的数量

        self.add_fids = []
        self.delete_fids = []
        self.update_fids = []

    def analyze_yesterday_diff(self, conn):
        today = datetime.datetime.today().date()
        today_str = today.strftime("%Y-%m-%d %H:%M:%S")
        yesterday = today + datetime.timedelta(days=-1)
        yesterday_str = yesterday.strftime("%Y-%m-%d %H:%M:%S")
        try:
            with conn.cursor() as cursor:
                sql = "SELECT es_id, fid FROM {table} WHERE c_date = %s and source = %s".format(table="ana_snap_job")
                cursor.execute(sql, (yesterday_str, self.source))
                yes_result = cursor.fetchall()

                sql = "SELECT es_id, fid FROM {table} WHERE c_date = %s and source = %s".format(table="ana_snap_job")
                cursor.execute(sql, (today_str, self.source))
                today_result = cursor.fetchall()
        except Exception as e:
            logging.error("analyze_yesterday_diff, search from mysql except: %s", e, exc_info=True)
            return

        self._analyze_yesterday_diff(yes_result, today_result)
        extend = {
            'add': self.add_fids,
            'delete': self.delete_fids,
            'update': self.update_fids
        }
        ct = datetime.datetime.today().strftime("%Y-%m-%d %H:%M:%S")
        data = {
            'source': self.source,
            'company': self.company,
            'c_date': today_str,
            'total_nu': self.total_nu,
            'add_nu': self.add_nu,
            'delete_nu': self.delete_nu,
            'update_nu': self.update_nu,
            'extend': json.dumps(extend),
            'ct': ct,
            'ut': ct
        }
        keys = ','.join(data.keys())
        values = ','.join(['%s'] * len(data))

        try:
            with conn.cursor() as cursor:
                sql = "INSERT INTO {table} ({keys}) VALUES ({values}) ON DUPLICATE KEY UPDATE total_nu=VALUES(total_nu), add_nu=VALUES(add_nu), delete_nu=VALUES(delete_nu), update_nu=VALUES(update_nu), extend=VALUES(extend)". \
                    format(table="ana_static_job", keys=keys, values=values)
                cursor.execute(sql, tuple(data.values()))
            conn.commit()
        except Exception as e:
            logging.error("analyze_yesterday_diff, save to mysql except: %s", e, exc_info=True)

    def _analyze_yesterday_diff(self, yes_result, today_result):
        y_es_ids, y_fids = set(), set()
        t_es_ids, t_fids = set(), set()
        for r in yes_result:
            y_es_ids.add(r[0])
            y_fids.add(r[1])
        for r in today_result:
            t_es_ids.add(r[0])
            t_fids.add(r[1])

        self.total_nu = len(today_result)
        es_ids_add = t_es_ids - y_es_ids
        fids_add = t_fids - y_fids
        self.add_nu = len(fids_add)
        self.add_fids = list(fids_add)

        es_ids_del = y_es_ids - t_es_ids
        fids_del = y_fids - t_fids
        self.delete_nu = len(fids_del)
        self.delete_fids = list(fids_del)

        u1 = set()
        for d in es_ids_add:
            u1.add(d.split('_')[1])
        fids_update = u1 - fids_add
        self.update_nu = len(fids_update)
        self.update_fids = list(fids_update)

    def analyze_recruitment_time(self, conn):
        today = datetime.datetime.today().date()
        three_month_ago = today + datetime.timedelta(days=-3*30)
        sql = 'SELECT * from ana_static_job where company = %s and c_date > %s order by c_date desc'

        try:
            with conn.cursor() as cursor:
                cursor.execute(sql, (self.company, str(three_month_ago)))
                all_res = cursor.fetchall()
        except Exception as e:
            logging.error("analyze_recruitment_time, search from mysql except: %s", e, exc_info=True)
            return

        total = 0
        m = {}
        for d in self.delete_fids:
            delete_day = today
            for item in all_res:
                add_day = item[3].date()
                if add_day >= delete_day:
                    continue
                extend = item[8]
                extend = json.loads(extend)
                adds = extend['add']
                if d in adds:
                    de = (delete_day - add_day).days
                    m[d] = [str(add_day), str(delete_day), de]
                    break
        total += len(m)
        if len(m) == 0:
            return
        fids = ','.join(m.keys())
        yes = delete_day + datetime.timedelta(days=-1)
        yes = yes.strftime("%Y-%m-%d %H:%M:%S")
        sql = 'SELECT * from ana_snap_job WHERE c_date = %s AND fid in ({fids})'.format(fids=fids)
        try:
            with conn.cursor() as cursor:
                cursor.execute(sql, yes)
                snap_res = cursor.fetchall()
                args = []
                for i in snap_res:
                    data = list(i)
                    data[0] = None
                    data[2] = datetime.datetime.today().date().strftime("%Y-%m-%d %H:%M:%S")
                    data[23] = datetime.datetime.today().strftime("%Y-%m-%d %H:%M:%S")
                    arg = data + m[data[3]]
                    args.append(arg)
                values = ','.join(['%s'] * 27)
                insert_sql = 'INSERT IGNORE INTO ana_duration_job VALUES ({values})'.format(values=values)
                cursor.executemany(insert_sql, args)
            conn.commit()
        except Exception as e:
            logging.error("analyze_recruitment_time, insert mysql except: %s", e, exc_info=True)
            return
        #print("total:", total)