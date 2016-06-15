from __future__ import print_function
import torndb
import random
import time

# 数据库连接
def get_mysql_conn():
    return torndb.Connection(
        host=mysql['host'] + ':' + mysql['port'],
        database=mysql['database'],
        user=mysql['user'],
        password=mysql['password'],
        charset=mysql['charset']
    )

# 数据库连接数据
mysql = {
    'host' : 'localdb.ckzqetgzhfi5.ap-southeast-1.rds.amazonaws.com',
    'port' : ‘XXXX’,
    'database' : ‘XXXX’,
    'password' : ‘XXXX’,
    'user' : ‘XXXX’,
    'charset' : 'utf8'
}


# ip转换成数字，上传使用
def ip2int(ip):
    try:
        hexn = ''.join(["%02X" % long(i) for i in ip.split('.')])
    except Exception, e:
        hexn = ''.join(["%02X" % long(i) for i in '0.0.0.0'.split('.')])
    return long(hexn, 16)

# 数字转换成ip，查询使用
def int2ip(n):
    d = 256 * 256 *256
    q = []
    while d > 0:
        m, n = divmod(n, d)
        q.append(str(m))
        d = d/256
    return '.'.join(q)

# 上传数据
def uploadData():
    with open('./ipdata.csv', 'r') as fr:
        line = fr.readlines()
    nl_p_list = []
    for l in line:
        ls = l.strip().split(',', 4)
        c1, c2, c3, c4, c5 = ls[0], ip2int(ls[1]), ip2int(ls[2]), ls[3], ls[4]
        nl = [c1, c2, c3, c4, c5]
        nl_p_list.append(nl)

    db = get_mysql_conn()
    db.execute("START TRANSACTION")

    block = 1000

    for i in range(len(nl_p_list) / block + 1):
        tmp_nl_p_list = nl_p_list[i * block: (i + 1) * block]
        db.insertmany('insert into ipdata(id, startip, endip, country, carrier) VALUES (%s, %s, %s, %s, %s)',
                      tmp_nl_p_list)
    db.execute("COMMIT")


# 随机查询，查看建立索引后的结果
def randomQuery(randomn):
    with open('./ipdata.csv', 'r') as fr:
        line = fr.readlines()
    nl_p_list = []
    for l in line:
        ls = l.strip().split(',', 4)
        c1, c2, c3, c4, c5 = ls[0], ip2int(ls[1]), ip2int(ls[2]), ls[3], ls[4]
        nl = [c1, c2, c3, c4, c5]
        nl_p_list.append(nl)
    ip_list = map(lambda x: x[1], random.sample(nl_p_list, randomn))
    db = get_mysql_conn()
    print ('Query is running...')

    ret_list = []
    for ip in ip_list:
        ret = db.get('select * from ipdata where %s <= startip order by startip Desc limit 1', ip)
        startip, endip = ret.get('startip'), ret.get('endip')
        if startip <= ip <= endip:
            ret_list.append((ip, ret.get('country')))
        else:
            ret_list.append((ip, u'unknow'))


if __name__ == '__main__':
    randomQuery(100)
