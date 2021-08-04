import time
from selenium import webdriver


def load_html_with_click(url, click_on_text=None, click_on_xpath=None):
    """
    :param url: url to be loaded
    :param click_on_text: if a click is required on a button with unique text
    :param click_on_xpath: if click is required using xpath (recommended for clicks)
    :return:
    """
    driver_path = '/Users/bhupi/PycharmProjects/splash_engine/chromedriver'
    driver = webdriver.Chrome(executable_path=driver_path)

    # url = "https://www.hardingloevner.com/ways-to-invest/us-mutual-funds/global-equity-portfolio/"
    driver.get(url)
    time.sleep(5)
    if click_on_text:
        element = driver.find_element_by_link_text(click_on_text).click()
        time.sleep(1)
    if click_on_xpath:
        element = driver.find_element_by_xpath(click_on_xpath).click()
        time.sleep(1)

    page_source = driver.page_source
    driver.close()
    driver.quit()
    return page_source
