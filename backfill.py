import pandas as pd
import psycopg2
import psycopg2.extras as extras
import os 
from dotenv import load_dotenv

load_dotenv()

names = ["drawNumber", "drawDate", "number1", "number2", "number3", "number4", "number5", "number6", "addNumber",
         "a","b","c","d","e","f","g","h","i","j",
         "g1tickets", "g1ShareAmt",
         "g2tickets", "g2ShareAmt",
         "g3tickets", "g3ShareAmt",
         "g4tickets", "g4ShareAmt",
         "g5tickets", "g5ShareAmt",
         "g6tickets", "g6ShareAmt",
         "g7tickets", "g7ShareAmt"]

usecols = ["drawNumber", "drawDate", "number1", "number2", "number3", "number4", "number5", "number6", "addNumber",
           "g1tickets", "g1ShareAmt",
           "g2tickets", "g2ShareAmt",
           "g3tickets", "g3ShareAmt",
           "g4tickets", "g4ShareAmt",
           "g5tickets", "g5ShareAmt",
           "g6tickets", "g6ShareAmt",
           "g7tickets", "g7ShareAmt"]

# dtype = {"drawNumber":"Int64", "drawDate":"object", "number1":"Int64", "number2":"Int64", "number3":"Int64", "number4":"Int64", "number5":"Int64", "number6":"Int64", "addNumber":"Int64",
#            "g1tickets":"Int64", "g1ShareAmt":"Int64",
#            "g2tickets":"Int64", "g2ShareAmt":"Int64",
#            "g3tickets":"Int64", "g3ShareAmt":"Int64",
#            "g4tickets":"Int64", "g4ShareAmt":"Int64",
#            "g5tickets":"Int64", "g5ShareAmt":"Int64",
#            "g6tickets":"Int64", "g6ShareAmt":"Int64",
#            "g7tickets":"Int64", "g7ShareAmt":"Int64"}

df = pd.read_csv('toto281025.csv',usecols=usecols, names=names, header=0)
df = df[309:]
df = df.replace({float('nan'): None})

sql = ''' INSERT INTO toto (
                        drawNumber,
                        drawDate,
                        number1,
                        number2,
                        number3,
                        number4,
                        number5,
                        number6,
                        addNumber,
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
                        g7tickets) VALUES %s
'''                        

conn = psycopg2.connect(host = os.getenv("PG_HOST"),
                        user = os.getenv("PG_USER"),
                        password = os.getenv("PG_PASSWORD"),
                        dbname = os.getenv("PG_DB"),
                        port = os.getenv("PG_PORT"))
cur = conn.cursor()

out = [tuple(x) for x in df.to_numpy()]

extras.execute_values(cur,sql,out)
conn.commit()





