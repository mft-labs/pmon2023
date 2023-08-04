#!/opt/B2B/anaconda3/bin/python
import pandas as pd
#import sqlite3
import psycopg2

# Connect to the db
#con = sqlite3.connect('dst_sfg_test_log.db')
con = psycopg2.connect(host="10.31.1.169", database="perf", user="eb2bgate", password="GFriXGXOXxj3GJKjpDGp")

