import asyncio
from time import sleep
from dataclasses import dataclass, fields
from selenium.webdriver.remote.webelement import WebElement
from selenium.webdriver import Chrome
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
import pandas as pd
import gspread_asyncio
from utils import get_creds, async_sheet_from_df
OLX_URL = "https://www.olx.ua/uk/nedvizhimost/kvartiry/prodazha-kvartir/?currency=UAH"

# @dataclass
# def FlatDTO():
#     price: int
#     floor: int
#     all_floors: int
#     locality: str
#     area: int



def get_webrdiver(
    headless: bool = True,
    detach: bool = False
):
    chrome_options = Options()
    if headless:
        chrome_options.add_argument("--headless")
    if detach:
        chrome_options.add_experimental_option("detach", True)
    chrome_options.add_argument("window-size=1400,600")
    driver = Chrome(options=chrome_options)

    return driver
def find_element(driver: Chrome, by: By, value: str) -> WebElement:
    wait = WebDriverWait(driver, 10)
    return wait.until(EC.presence_of_element_located((by, value)))

def find_elements(driver: Chrome, by: By, value: str) -> list[WebElement]:
    wait = WebDriverWait(driver, 10)
    return wait.until(EC.presence_of_all_elements_located((by, value)))

def get_next_page(driver: Chrome) -> str:
    next_page_url = find_element(driver, By.CSS_SELECTOR, "a[data-cy='pagination-forward']").get_attribute("href")
    return next_page_url

def get_flat_urls(driver: Chrome, url: str) -> list[str]:
    driver.get(url=url)
    flats = driver.find_elements(by=By.CSS_SELECTOR, value="div[data-cy='l-card']>a")
    return [flat.get_attribute("href") for flat in flats]

def parse_flat(url: str, driver: Chrome, df: pd.DataFrame) -> dict:
    driver.get(url)
    df.loc[len(df)]=dict(
        price=find_element(driver, By.CSS_SELECTOR, "h3.css-12vqlj3").text,
        floor=find_element(driver, By.XPATH, "//p[contains(text(), 'Поверх:')]").text.split(" ")[1],
        all_floors=find_element(driver, By.XPATH, "//p[contains(text(), 'Поверховість:')]").text.split(" ")[1],
        locality=find_elements(driver, By.XPATH, "//section")[1].text,
        area=find_element(driver, By.XPATH, "//p[contains(text(), 'Загальна площа:')]").text.split(" ")[2],
    )

async def parse_flats(pages: int = 1, sheet_url: str = None):
    flats_df = pd.DataFrame(columns=[
    "price",
    "floor",
    "all_floors",
    "locality",
    "area"
    ]
)
    driver = get_webrdiver(headless=True, detach=False)
    next_page = OLX_URL
    while next_page and pages:
        print(next_page, pages)
        pages -= 1
        flats_urls = get_flat_urls(driver=driver, url=next_page)
        next_page = get_next_page(driver)
        [parse_flat(url, driver, flats_df) for url in flats_urls]
    driver.quit()
    if not sheet_url:
        return

    agcm = gspread_asyncio.AsyncioGspreadClientManager(get_creds)
    client = await agcm.authorize()

    spreadsheet = await client.open_by_url(sheet_url)
    worksheet = (await spreadsheet.worksheets())[0]
    await async_sheet_from_df(worksheet=worksheet, dataframe=flats_df)
    

if __name__ == "__main__":
    asyncio.run(parse_flats(pages=1, sheet_url="https://docs.google.com/spreadsheets/d/1lY0sYzoVTeILkiz2S6gI3mfULvIwGB0aqUaaRSOynnE/edit?usp=sharing"))




