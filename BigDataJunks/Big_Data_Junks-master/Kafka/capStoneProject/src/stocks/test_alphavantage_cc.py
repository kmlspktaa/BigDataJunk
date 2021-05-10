
"""
Unit Test class
11/07/2020
Author@ Abuchi Okeke
"""
import unittest
import stocks.aggregate_stocks as al
#from stocks import aggregate_stocks as al

import sys
sys.path.append('file:///home/fieldemployee/Big_Data_Training/Kafka/capStoneProject')

class TestAlphaVantage(unittest.TestCase):
    def test_stock(self):
        self.assertAlmostEqual(al.sum_plotting_data().collect()[0]["sum(close)"], al.stocks_hive_tb().collect()[0]["sum(close)"])
        self.assertAlmostEqual(al.sum_plotting_data().collect()[0]["sum(SMA1)"], al.stocks_hive_tb().collect()[0]["sum(SMA1)"])
        self.assertAlmostEqual(al.sum_plotting_data().collect()[0]["sum(SMA2)"],al.stocks_hive_tb().collect()[0]["sum(SMA2)"])



