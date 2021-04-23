import pandas as pd
import numpy as np
from bs4 import BeautifulSoup
import requests
import re
from selenium import webdriver
import chromedriver_binary
from webdriver_manager.chrome import ChromeDriverManager
import time

import yfinance as yf

def scraper_to_statement(link):
    headers = []
    temp_list = []
    final = []
    index = 0

    # pull data from link
    page_response = requests.get(link, timeout=1000)
    # structure raw data for parsing
    page_content = BeautifulSoup(page_response.content)
    # filter for items we want
    features = page_content.find_all('div', class_='D(tbr)')

    # create headers
    for item in features[0].find_all('div', class_='D(ib)'):
        headers.append(item.text)

    # statement contents
    while index <= len(features) - 1:
        # filter for each line of the statement
        temp = features[index].find_all('div', class_='D(tbc)')
        for line in temp:
            # each item adding to a temporary list
            temp_list.append(line.text)
        # temp_list added to final list
        final.append(temp_list)
        # clear temp_list
        temp_list = []
        index += 1

    df = pd.DataFrame(final[1:])
    df.columns = headers

    return df


# def yahoo_statement(symbol):
#     is_link = 'https://finance.yahoo.com/quote/%s/financials?p=%s' % (symbol, symbol)
#     bs_link = 'https://finance.yahoo.com/quote/%s/balance-sheet?p=%s' % (symbol, symbol)
#     cf_link = 'https://finance.yahoo.com/quote/%s/cash-flow?p=%s' % (symbol, symbol)
#     df_statement = scraper_to_statement( is_link)
#     return df_statement



def _scraper_to_statement(_webdriver):
# Web page fetched from driver is parsed using Beautiful Soup.
    headers = []
    temp_list = []
    final = []
    index = 0

    page_content = BeautifulSoup(_webdriver.page_source, 'html.parser')

    # filter for items we want
    features = page_content.find_all('div', class_='D(tbr)')

    # create headers
    for item in features[0].find_all('div', class_='D(ib)'):
        headers.append(item.text)

    # statement contents
    while index <= len(features) - 1:
        # filter for each line of the statement
        temp = features[index].find_all('div', class_='D(tbc)')
        for line in temp:
            # each item adding to a temporary list
            temp_list.append(line.text)
        # temp_list added to final list
        final.append(temp_list)
        # clear temp_list
        temp_list = []
        index += 1

    df = pd.DataFrame(final[1:])
    df.columns = headers

    return df


def _yahoo_statement(url):

    try:
        # WebDriver
        driver = webdriver.Chrome('C:\WebDriver\chromedriver.exe')
        driver.get(url)

        # Click "Quarterly"
        driver.find_element_by_xpath("//span[text()='Quarterly']").click()
        time.sleep(1)

        # Driver scrolls down three times to load the table.
        for i in range(0, 3):
            driver.execute_script("window.scrollBy(0,5000)")
            time.sleep(1)


        df_page  = _scraper_to_statement(driver)

        # Fetch the webpage and store in a variable.
        # webpage = driver.page_source
        # print the fetched webpage.
        # print(webpage)

    except errno as e:
        print( e )
    finally:
        driver.close()

    return df_page


def yahoo_finance_statement(symbol):
    url = 'https://finance.yahoo.com/quote/%s/financials?p=%s' % (symbol, symbol)
    df_statement = _yahoo_statement( url )
    return df_statement


def yahoo_balancesheeet_statement(symbol):
    url = 'https://finance.yahoo.com/quote/%s/balance-sheet?p=%s' % (symbol, symbol)
    df_statement = _yahoo_statement( url )
    return df_statement


def yahoo_cahflow_statement(symbol):
    cf_link = 'https://finance.yahoo.com/quote/%s/cash-flow?p=%s' % (symbol, symbol)
    df_statement = _yahoo_statement( url )
    return df_statement

def yahoo_historical_price (symbol):
    data = yf.download(symbol,'2021-01-01', '2021-04-10')
    print(data.tail())

def yahoo_intraday_price(symbol):
    url = 'https://finance.yahoo.com/quote/%s/financials?p=%s' % (symbol, symbol)
    user_agent = {'User-agent': 'Mozilla/5.0'}
    response = requests.get(url, headers=user_agent)
    driver = webdriver.Chrome('C:\WebDriver\chromedriver.exe')
    driver.get(url)
    html = driver.execute_script("return document.body.innerHTML;")
    soup = BeautifulSoup(html, 'lxml')
    close_price = [entry.text for entry in
                   soup.find_all('span', {'class': 'Trsdu(0.3s) Fw(b) Fz(36px) Mb(-4px) D(ib)'})]
    after_hours_price = [entry.text for entry in soup.find_all('span', {'class': 'C($primaryColor) Fz(24px) Fw(b)'})]

    return close_price, after_hours_price


if __name__ == '__main__':
    symbol = 'AAPL'

    # 3. Get Historical Price
    yahoo_historical_price(symbol)
    #  2. Get Intraday Price
    # close_price, after_hours_price = yahoo_intraday_price(symbol)
    # print(close_price)
    # print(after_hours_price)


    #  1. Get Finance Statement
    # df_statement = yahoo_statement('AAPL')
    # print(df_statement)

#---------------------------------------------------------------
# options = Options()
# options.add_argument("start-maximized")
# driver = webdriver.Chrome(chrome_options=options, executable_path=r'C:\Utility\BrowserDrivers\chromedriver.exe')
# ticker_list = ["AAPL"]
# for ticker in ticker_list:
#     url = "https://finance.yahoo.com/quote/{}/financials?p={}".format(ticker, ticker)
#     driver.get(url)
#     WebDriverWait(driver, 3600).until(EC.element_to_be_clickable((By.XPATH, "//section[@data-test='qsp-financial']//span[text()='Quarterly']"))).click()