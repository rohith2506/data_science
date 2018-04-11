import MySQLdb
from pprint import pprint
from datetime import datetime
import numpy
import pdb

class FeatureExtractor():
    def __init__(self):
        self.region_dict = {}
        self.variety_dict = {}
        self.state_id = 0

    def fill_variety_dict(self, variety):
        if variety not in self.variety_dict:
            self.variety_dict[variety] = None

    def get_district_id(self, state):
        prev_districts = self.region_dict[state]['districts'].keys()
        return len(prev_districts) + 1

    def get_market_id(self, state, district):
        prev_markets = self.region_dict[state]['districts'][district]['markets'].keys()
        return len(prev_markets) + 1

    def fill_region_dict(self, state, district, market):
        try:
            if state not in self.region_dict:
                # State
                self.region_dict[state] = {}
                self.state_id = self.state_id + 1
                self.region_dict[state]['state_id'] = self.state_id
                # District
                self.region_dict[state]['districts'] = {}
                self.region_dict[state]['districts'][district] = {}
                self.region_dict[state]['districts'][district]['district_id'] = 1
                # Market
                self.region_dict[state]['districts'][district]['markets'] = {}
                self.region_dict[state]['districts'][district]['markets'][market] = {}
                self.region_dict[state]['districts'][district]['markets'][market]['market_id'] = 1
            elif district not in self.region_dict[state]['districts'].keys():
                # District
                district_id = self.get_district_id(state)
                self.region_dict[state]['districts'][district] = {}
                self.region_dict[state]['districts'][district]['district_id'] = district_id
                # Market
                self.region_dict[state]['districts'][district]['markets'] = {}
                self.region_dict[state]['districts'][district]['markets'][market] = {}
                self.region_dict[state]['districts'][district]['markets'][market]['market_id'] = 1
            elif market not in self.region_dict[state]['districts'][district]['markets'].keys():
                # Market
                market_id = self.get_market_id(state, district)
                self.region_dict[state]['districts'][district]['markets'][market] = {}
                self.region_dict[state]['districts'][district]['markets'][market]['market_id'] = market_id
        except Exception, e:
            print "Error in processing regions"

    def get_region_ids(self, state, district, market):
        state_id = self.region_dict[state]['state_id']
        district_id = self.region_dict[state]['districts'][district]['district_id']
        market_id = self.region_dict[state]['districts'][district]['markets'][market]['market_id']
        return state_id, district_id, market_id

    def push_data_to_mysql(self, db, cursor, output):
        state_id, district_id, market_id, day_of_the_week, min_price, max_price, median_price = output
        insert_query = "insert into parsed_data (state_id, district_id, market_id, day_of_the_week, min_price, max_price, median_price) values (%d, %d, %d, %d, %f, %f, %f)" \
                        %(state_id, district_id, market_id, day_of_the_week, min_price, max_price, median_price)
        cursor.execute(insert_query)
        db.commit()

    def remove_outliers(self, numbers):
        numbers.sort()
        q2 = numpy.median(numbers)
        first, second = numbers[0:len(numbers)/2], numbers[len(numbers)/2:]
        q1, q3 = numpy.median(first), numpy.median(second)
        iqr = q3 - q1
        result = []
        for number in numbers:
            lower, greater = (q1 - 1.5 * iqr), (q3 + 1.5 * iqr)
            if number < lower or number > greater: number = q2
            result.append(number)
        return result

def get_mysql_cursor():
    db, cursor = None, None
    try:
        db = MySQLdb.connect(host='localhost', db='agri', user='root', passwd='')
        cursor = db.cursor()
    except Exception, e:
        print "Error in connecting to mysql: ", e
    return db, cursor

def process():
    db, cursor = get_mysql_cursor()
    get_query = "select state, district, market, commodity, variety, arrival_date, min_price, max_price, median_price from commodity_data"
    cursor.execute(get_query)
    row = cursor.fetchone()
    f = FeatureExtractor()
    results = []
    print "Processing region info......."
    while row:
        try:
            row = cursor.fetchone()
            results.append(row)
            f.fill_region_dict(row[0], row[1], row[2])
        except Exception, e:
            pass
    print "Done!!!!!!"
    results = results[:len(results)-1]
    formatted_data, min_prices, max_prices, median_prices = [], [], [], []
    for result in results:
        state, district, market, commodity, variety, arrival_date, min_price, max_price, median_price = result
        state_id, district_id, market_id = f.get_region_ids(state, district, market)
        day_of_the_week = arrival_date.weekday()
        p1, p2 = arrival_date.isoformat(), datetime.today().strftime("%Y-%m-%d")
        if p1 > p2: continue
        formatted_data.append([state_id, district_id, market_id, day_of_the_week, min_price, max_price, median_price])
        min_prices.append(min_price)
        max_prices.append(max_price)
        median_prices.append(median_price)
    min_prices = f.remove_outliers(min_prices)
    max_prices = f.remove_outliers(max_prices)
    median_prices = f.remove_outliers(median_prices)
    for i in range(len(formatted_data)):
        formatted_data[i][4], formatted_data[i][5], formatted_data[i][6] = min_prices[i], max_prices[i], median_prices[i]
    print "pushing formatted data to mysql......"
    for output in formatted_data:
        f.push_data_to_mysql(db, cursor, output)
    print "Done!!!!!!!!!"

if __name__ == "__main__":
    process()
