import quandl
import matplotlib.pyplot as plt
import pandas as pd
import numpy as np

# ---------------------------------------------------------------------
# cannot use it bc  free-version does not provide the latest market data
# ---------------------------------------------------------------------
quandl.ApiConfig.api_key="Mg29-LcqDTh3L8h14yx8"
# Every EOD data has the Quandl code in the format EOD/{TICKER}
data = quandl.get("EOD/AAPL",  start_date="2020-01-01", end_date="2021-04-09")
# data = quandl.get("WIKI/AAPL", start_date="2020-01-01", end_date="2021-04-09")

# Plot the prices
# data.to_csv("c:/temp/AAPL_Quandl_2.csv")
print(data.tail())