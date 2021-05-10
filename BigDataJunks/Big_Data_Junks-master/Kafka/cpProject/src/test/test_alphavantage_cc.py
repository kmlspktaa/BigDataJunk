
"""
Unit Test class
11/07/2020
Author@ Abuchi Okeke
"""
import unittest
import warnings
import stocks.aggregate_stocks as al

# import sys
# sys.path.append('/home/fieldemployee/Big_Data_Training/Kafka/cpProject/')
import configparser
from datetime import datetime

# set database configuration path here
auth_path = '/home/fieldemployee/bin/conf/db.ini'

db1 = 'mysql'

# Get current date time
date_time = (datetime.now().strftime('%Y%m%d%H%M%S'))
config = configparser.ConfigParser()
config.read(auth_path)

# sqlserver
sqlserver_user = config['sqlserver']['user']
sqlserver_password = config['sqlserver']['password']
sqlserver_driver_format = config['sqlserver']['driver_format']
#sqlserver_url = config['sqlserver']['url']
sqlserver_url = 'jdbc:sqlserver://localhost:1433;databaseName=stockdb'
sqlserver_connector_path = config['sqlserver']['driver_connector']

class TestAlphaVantage(unittest.TestCase):

    def test_sum(self):
        warnings.filterwarnings(action="ignore", message="unclosed", category=ResourceWarning)
        self.assertAlmostEqual(al.sum_plotting_data().collect()[0]["sum(close)"], al.stocks_hive_tb().collect()[0]["sum(close)"])
        self.assertAlmostEqual(al.sum_plotting_data().collect()[0]["sum(SMA1)"], al.stocks_hive_tb().collect()[0]["sum(SMA1)"])
        self.assertAlmostEqual(al.sum_plotting_data().collect()[0]["sum(SMA2)"], al.stocks_hive_tb().collect()[0]["sum(SMA2)"])

        # if pass testcases do this
        print("Passed")
        al.save_to_rdbms(al.stocks_sqlserver_tb, "stocks_production", sqlserver_user, sqlserver_password, sqlserver_driver_format, sqlserver_url)


    # def test_SMA1_sum(self):
    #     self.assertAlmostEqual(al.sum_plotting_data().collect()[0]["sum(SMA1)"], al.stocks_hive_tb().collect()[0]["sum(SMA1)"])
    #     warnings.filterwarnings(action="ignore", message="unclosed", category=ResourceWarning)
    #     print("Passed")
    #
    # def test_SMA2_sum(self):
    #     self.assertAlmostEqual(al.sum_plotting_data().collect()[0]["sum(SMA2)"], al.stocks_hive_tb().collect()[0]["sum(SMA2)"])
    #     warnings.filterwarnings(action="ignore", message="unclosed", category=ResourceWarning)
    #     print("Passed")


if __name__ == '__main__':
    unittest.main(verbosity=2)






