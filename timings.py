from pathlib import Path
from selenium import webdriver
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support import expected_conditions
from selenium.webdriver.support.ui import WebDriverWait
from time import sleep
from timeit import timeit
from trino.dbapi import connect


# HOST = "localhost"
# DB_PORT, FLUKE_PORT = 7700, 7701
# HOST = "52.191.100.80"
HOST = input("Enter DB host: ")
DB_PORT, FLUKE_PORT = 80, 80

BASE_URL = "http://" + HOST
DB_URL = f"{BASE_URL}:{DB_PORT}"
API_EXPLORER_URL = DB_URL + "/ui/resurface/"
FLUKE_URL = f"{BASE_URL}:{FLUKE_PORT}" + ("/fluke/" if DB_PORT == 80 and FLUKE_PORT == 80 else "/")
USERNAME = "importer"

BMID = input("Enter the identifier for this benchmark: ")

UI_VIEWS_BUTTON_ID = "headlessui-listbox-button-2"
UI_CHARTS_DIV_CLASS = "space-y-8"
VIEWS_DICT = {24: "Method Injection Attacks", 0: "All API Calls", 2: "Completed Calls"}


def is_saturated(browser):
    return browser.execute_script('location.reload(); return document.body.childNodes[0].data === "true";')


path = Path("./benchmarks/" + BMID)
path.mkdir(parents=True, exist_ok=True)

with open(str(path) + "/" + BMID + ".csv", 'w') as outf:
    print("view_name", "traces_ms", "trends_ms", "count_ms", sep=",", file=outf)

browser = webdriver.Firefox()
# browser.maximize_window()

# API Explorer log in
browser.get(API_EXPLORER_URL)
browser.find_element(By.NAME, 'username').send_keys(USERNAME + Keys.RETURN)
apix = browser.current_window_handle

# Check if DB is saturated by opening a new tab to the fluke endpoint
browser.switch_to.new_window('tab')
browser.get(FLUKE_URL + "saturated")

try:
    WebDriverWait(browser, timeout=300, poll_frequency=1).until(is_saturated)
except TimeoutException:
    print("Timed out waiting for saturation")
else:
    browser.close()
    browser.switch_to.window(apix)

# sleep(20)  # manually wait for the UI to load for the first time
# Wait for charts to load
WebDriverWait(browser, 30).until(expected_conditions.visibility_of_element_located((By.CLASS_NAME, UI_CHARTS_DIV_CLASS)))

# Perform COUNT(*) query using the trino API
print("Performing trino count query...")
conn = connect(host=HOST,port=DB_PORT,user=USERNAME,catalog="resurface",schema="data")
cur = conn.cursor()

def count():
    cur.execute("SELECT COUNT(*) FROM resurface.data.message")
    return cur.fetchall()


interval = timeit(count, number=1)
cur.execute("SELECT COUNT(*) FROM resurface.data.message")
saturated_count = cur.fetchone()[0]
conn.close()

with open(str(path) + "/" + BMID + ".txt", "w") as outf:
    print(BMID, f"Saturated at: {saturated_count} calls", f"COUNT(*) time [s]: {interval}", sep='\n', file=outf)

print(f"DB COUNT(*) time [s]: {interval}")

# Perform queries using the UI
timings = browser.execute_script("return window.performance.getEntries();")
query_timings = list(filter(lambda x: 'query' in x['name'], timings))
old_query_timings = list(filter(lambda x: 'query' in x['name'], timings))

views_button = WebDriverWait(browser, 10).until(expected_conditions.visibility_of_element_located((By.ID, UI_VIEWS_BUTTON_ID)))
for i, name in VIEWS_DICT.items():
    views_button.click()

    views_dropdown_list = browser.find_element(By.TAG_NAME, "ul")
    views = views_dropdown_list.find_elements(By.TAG_NAME, "li")
    views[i].click()
    WebDriverWait(browser, 30).until(expected_conditions.visibility_of_element_located((By.CLASS_NAME, UI_CHARTS_DIV_CLASS)))
    sleep(10)  # wait for count query manually
    timings = browser.execute_script("return window.performance.getEntries();")
    query_timings = list(filter(lambda x: ('query' in x['name']) and (x not in old_query_timings), timings))
    old_query_timings.extend(query_timings)
    assert len(query_timings) in (2, 3)
    sorted_timings = list(map(lambda x: x['responseStart'] - x['requestStart'], sorted(query_timings, key=(lambda x: x['encodedBodySize']), reverse=True)))
    
    if len(sorted_timings) == 3:
        traces, trends, counts = sorted_timings
    else:
        traces, trends = sorted_timings
        counts = ''
    
    with open(str(path) + "/" + BMID + ".csv", 'a') as outf:
        print(name, traces, trends, counts, sep=",", file=outf)
    
    print(name, f"TRACES (ms): {traces}", f"TRENDS (ms): {trends}", f"COUNT (ms): {counts if counts else '-'}", "", sep='\n')

browser.quit()