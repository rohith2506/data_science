import MySQLdb
import os
import pdb


def get_mysql_cursor():
    db, cur = None, None
    try:
        db = MySQLdb.connect(host='localhost', db='agri', user='root', passwd='')
        cur = db.cursor()
    except Exception, e:
        print "Error in creating mysql connection: ", str(e)
    return db, cur

def modify_line(line):
    result = ""
    start, end = 0, 1
    for i in range(0, len(line)):
        if line[i] == '"' and end == 1:
            start, end = 1, 0
        elif line[i] == '"' and start == 1:
            start, end = 0, 1
        if line[i] != ',':
            result = result + line[i]
        else:
            if start == 1 and end == 0:
                result = result + " "
            else:
                result = result + line[i]
    return result

def insert_data_to_mysql(db, cursor, line, state, district, market, commodity, variety, arrival_date, min_price, max_price, mode_price):
    is_inserted = True
    try:
        arrival_date = arrival_date.replace('/', '-')
        day, month, year = arrival_date.split('-')
        arrival_date = year + "-" + month + "-" + day
        min_price, max_price, mode_price = float(min_price), float(max_price), float(mode_price)
        insert_query = "insert into commodity_data (state, district, market, commodity, variety, arrival_date, min_price, max_price, median_price) \
                        values ('%s', '%s', '%s', '%s', '%s', '%s', %f, %f, %f)" %(state, district, market, commodity, variety, arrival_date, min_price, max_price, mode_price)
        cursor.execute(insert_query)
        db.commit()
    except Exception, e:
        is_inserted = False
    return is_inserted

def import_data_from_csv_to_mysql():
    db, cursor = get_mysql_cursor()
    dir_path = "../data"
    data_files = os.listdir(dir_path)
    err_cnt, err_insert_cnt, err_lines = 0, 0, []
    for data_file in data_files:
        file_path = dir_path + "/" + data_file
        if 'all' not in file_path: continue
        line_cnt = 0
        for line in open(file_path).readlines():
            mline = modify_line(line.strip())
            values = mline.split(",")
            try:
                state, district, market, arrival_date, min_price, max_price, mode_price = values
                line_cnt = line_cnt + 1
                if line_cnt == 1: continue
                is_inserted = insert_data_to_mysql(db, cursor, mline, state, district, market, 'banana', 'other', arrival_date, min_price, max_price, mode_price)
                if not is_inserted:
                    err_insert_cnt = err_insert_cnt + 1
                    err_lines.append(line)
            except Exception, e:
                pdb.set_trace()
                err_cnt = err_cnt + 1
        print "File processed: ", file_path
        print "Number of lines inserted: ", line_cnt
        print "##################################################################"
    print "Error parse count: ", err_cnt
    print "Error insert count: ", err_insert_cnt
    print "error lines: ", err_lines

if __name__ == "__main__":
    import_data_from_csv_to_mysql()
