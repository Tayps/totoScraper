from selenium import webdriver
from selenium.webdriver.common.by import By
from datetime import datetime 
import os
import psycopg2
from dotenv import load_dotenv
from selenium.webdriver.support.ui import Select

load_dotenv()
driver = webdriver.Chrome()
driver.implicitly_wait(3)

dataInsert = ''' INSERT INTO toto (
                        drawNumber,
                        drawDate,
                        number1,
                        number2,
                        number3,
                        number4,
                        number5,
                        number6,
                        addNumber,
                        jackpot,
                        g1ShareAmt,
                        g2ShareAmt,
                        g3ShareAmt,
                        g4ShareAmt,
                        g5ShareAmt,
                        g6ShareAmt,
                        g7ShareAmt,
                        g1tickets,
                        g2tickets,
                        g3tickets,
                        g4tickets,
                        g5tickets,
                        g6tickets,
                        g7tickets) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
'''

# Connect to DB
conn = psycopg2.connect(host = os.getenv("PG_HOST"),
                        user = os.getenv("PG_USER"),
                        password = os.getenv("PG_PASSWORD"),
                        dbname = os.getenv("PG_DB"),
                        port = os.getenv("PG_PORT"))
cur = conn.cursor()

# Retrieve last draw recorded in DB. Returns an integer
def getDB():
    cur.execute("SELECT MAX(drawnumber) FROM toto")
    drawDB = cur.fetchall()[0][0]
    return drawDB

# Retrieve last draw from singapore pools, returns an integer
def getSP():
    driver.get("https://www.singaporepools.com.sg/en/product/sr/pages/toto_results.aspx")
    driver.set_window_size(1024,1024)
    drawSP = driver.find_element(By.XPATH, "/html/body/form/div[5]/div[1]/div/section/div/div/div/span/section/div/div[2]/div[2]/div/div/div[2]/div[1]/div/div[1]/div/div[2]/div[1]/table[1]/thead/tr/th[2]")
    drawSP = int(drawSP.text.replace('Draw No. ', ''))
    return drawSP

#Compare latest draw in DB with Singapore Pools, return Bool.
def dataLag():
    SP = getSP()
    DB = getDB()
    datalag = (SP > DB) #mismatch if SP data is more than DB data
    return datalag 

# Retrieve draw data from SP, returns a dictionary.
def dataRetrieve():
    drawData = {}
    drawData["drawNumber"] = int(driver.find_element(By.XPATH, "/html/body/form/div[5]/div[1]/div/section/div/div/div/span/section/div/div[2]/div[2]/div/div/div[2]/div[1]/div/div[1]/div/div[2]/div[1]/table[1]/thead/tr/th[2]").text.replace('Draw No. ', ''))
    drawData["drawDate"] = datetime.strptime(driver.find_element(By.XPATH,"/html/body/form/div[5]/div[1]/div/section/div/div/div/span/section/div/div[2]/div[2]/div/div/div[2]/div[1]/div/div[1]/div/div[2]/div[1]/table[1]/thead/tr/th[1]").text, "%a, %d %b %Y").date()
    for n in range (1,7):
        drawData[f"winNo{n}"] = int(driver.find_element(By.XPATH, "/html/body/form/div[5]/div[1]/div/section/div/div/div/span/section/div/div[2]/div[2]/div/div/div[2]/div[1]/div/div[1]/div/div[2]/div[1]/table[2]/tbody/tr/td["+str(n)+"]").text)
    drawData["addNumber"] = int(driver.find_element(By.XPATH, "/html/body/form/div[5]/div[1]/div/section/div/div/div/span/section/div/div[2]/div[2]/div/div/div[2]/div[1]/div/div[1]/div/div[2]/div[1]/table[3]/tbody/tr/td").text)
    drawData["jackpot"] = int(driver.find_element(By.XPATH, "/html/body/form/div[5]/div[1]/div/section/div/div/div/span/section/div/div[2]/div[2]/div/div/div[2]/div[1]/div/div[1]/div/div[2]/div[1]/table[4]/tbody/tr/td").text.replace('$','').replace(',',''))
    for row in range(2,9):
        drawData[f"g{row-1}ShareAmt"] = int(driver.find_element(By.XPATH, "/html/body/form/div[5]/div[1]/div/section/div/div/div/span/section/div/div[2]/div[2]/div/div/div[2]/div[1]/div/div[1]/div/div[2]/div[1]/table[5]/tbody/tr["+str(row)+"]/td[2]").text.replace('$','').replace(',','').replace('-','0'))
    for row in range(2,9):
        drawData[f"g{row-1}WinShares"] = int(driver.find_element(By.XPATH, "/html/body/form/div[5]/div[1]/div/section/div/div/div/span/section/div/div[2]/div[2]/div/div/div[2]/div[1]/div/div[1]/div/div[2]/div[1]/table[5]/tbody/tr["+str(row)+"]/td[3]").text.replace(',','').replace('-','0'))
    return drawData

#  Retrieval of data
def scrapeData():
    if dataLag():
        DB = getDB()
        missing = getSP() - DB
        for draw in range(1, missing +1):
            options = Select(driver.find_element(By.XPATH, "/html/body/form/div[5]/div[1]/div/section/div/div/div/span/section/div/div[2]/div[2]/div/div/div[2]/div[1]/div/div[1]/div/div[1]/div[2]/div/select"))
            options.select_by_value(f"{DB + draw}")
            insertVal = list(dataRetrieve().values())
            print(insertVal)
            cur.execute(dataInsert, insertVal)
            conn.commit()

def main():
    # dbconnect()
    scrapeData()

if __name__ == '__main__':
    main()