from sklearn.linear_model import LinearRegression
from sklearn import neighbors
import math
import MySQLdb
import pdb
import random

class KNN():
    def __init__(self):
        self.train_x, self.train_y = [], []
        self.test_x, self.test_y = [], []
        self.n_neighbors = 10

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

    def get_distance(self, x, y):
        result = 0.0
        for a, b in zip(x, y): result = result + (a-b) * (a-b)
        return math.sqrt(result)

    def get_neighbors(self, inp):
        distance_arr = []
        for x, y in zip(self.train_x, self.train_y):
            distance = self.get_distance(inp, x)
            distance_arr.append((distance, y))
        distance_arr.sort(key=lambda x:x[0])
        result = 0
        for i in range(min(len(distance_arr), self.n_neighbors)): result = result + distance_arr[i][1]
        result = result * 1.0 / min(len(distance_arr), self.n_neighbors)
        return result

    def solve(self, acceptable_range):
        cnt = 0
        for a, b in zip(self.test_x, self.test_y):
            predict_b = self.get_neighbors(a)
            print b, predict_b
            if abs(b - predict_b) <= acceptable_range: cnt = cnt + 1
        print "Accuracy: %.4f\n" %(cnt * 100.0 / len(self.test_y))

if __name__ == "__main__":
    k = KNN()
    print "Enter to the world of KNN!!!!!!!!!!!!!"
    print "percentage\tacceptable range\tparameter"
    percentage, acceptable_range, parameter = raw_input().split()
    percentage, acceptable_range = float(percentage), int(acceptable_range)
    k.prepare_data(parameter, percentage)
    k.solve(acceptable_range)
