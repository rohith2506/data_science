'''
Linear regression is not working out.

Reasons:
1) This is because i am trying to generalize the prices of banana all acorss india instead of focusing on the region
'''


from sklearn.linear_model import LinearRegression
import MySQLdb
import pdb
import random

class LinearReg():
    def __init__(self):
        self.train_x, self.train_y = [], []
        self.test_x, self.test_y = [], []
        self.lr = LinearRegression(normalize=True)

    def get_mysql_cursor(self):
        db, cursor = None, None
        try:
            db = MySQLdb.connect(host='localhost', db='agri', user='root', passwd='')
            cursor = db.cursor()
        except Exception, e:
            print "Error in connecting to mysql: ", e
        return db, cursor

    def prepare_data(self, which_y, percentage):
        db, cursor = self.get_mysql_cursor()
        query = "select state_id, district_id, market_id, day_of_the_week, min_price, max_price, median_price from parsed_data"
        cursor.execute(query)
        row = cursor.fetchone()
        results = []
        print "reading from mysql....."
        while row:
            try:
                results.append(row)
                row = cursor.fetchone()
            except:
                pass
        print "Done!!!!!!!!"
        print "length: ", len(results)
        print "Shuffling data....."
        random.shuffle(results)
        print "Done!!!!!!!!"
        partition_index = int(len(results) * percentage)
        print "parition_index: ", partition_index
        train_data, test_data = results[0:partition_index], results[partition_index:]
        for result in train_data:
            state_id, district_id, market_id, day_of_the_week, min_price, max_price, median_price = result
            self.train_x.append([state_id, district_id, market_id, day_of_the_week])
            if which_y == "min_price":
                self.train_y.append(min_price)
            elif which_y == "max_price":
                self.train_y.append(max_price)
            elif which_y == "median_price":
                self.train_y.append(median_price)
        for result in test_data:
            state_id, district_id, market_id, day_of_the_week, min_price, max_price, median_price = result
            self.test_x.append([state_id, district_id, market_id, day_of_the_week])
            if which_y == "min_price":
                self.test_y.append(min_price)
            elif which_y == "max_price":
                self.test_y.append(max_price)
            elif which_y == "median_price":
                self.test_y.append(median_price)
        print "train: ", len(self.train_x), len(self.train_y)
        print "test: ", len(self.test_x), len(self.test_y)

    def train(self):
        print "Training data......."
        self.lr.fit(self.train_x, self.train_y)
        print "Done!!!!!!!!!!"

    def test(self, acceptable_range):
        print "Testing data....."
        cnt = 0
        pred_test = self.lr.predict(self.test_x)
        for a, b in zip(self.test_y, pred_test):
            if abs(a - b) <= acceptable_range: cnt = cnt + 1
        print "Done!!!!!!!"
        print "Accuracy within acceptable range %d: %f" %(acceptable_range, (cnt * 100.0 / len(pred_test)))

if __name__ == "__main__":
    l = LinearReg()
    print "Enter to the world of linear regression!!!!!!!!!!!!!"
    print "percentage\tacceptable range\tparameter"
    percentage, acceptable_range, parameter = raw_input().split()
    percentage, acceptable_range = float(percentage), int(acceptable_range)
    l.prepare_data(parameter, percentage)
    l.train()
    l.test(acceptable_range)
