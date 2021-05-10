
"""
Unit Test class
11/07/2020
Author@ Abuchi Okeke
"""
import unittest
import aggregate_stocks as al

class TestAlphaVantage(unittest.TestCase):
    def test_stock(self):
        self.assertAlmostEqual(al.sum_plotting_data(), 0)



