import mysql.connector
import pandas as pd
import os

# DB CONFIG
conn = mysql.connector.connect(
    host="10.47.150.144",
    user="sqaviewjbr",
    password="5qAv13wJbr@2019#",
    database="kpi"
)
# QUERY
query = '''SELECT a.`tanggal`, a.`siteid`, ROUND(SUM(a.`user_number_max`)) User_4G, ROUND(SUM(a.`user_active_max_busy_hour`)) User_4G_BH,
ROUND(AVG(a.`prb_dl_used`),2) Util_4G_BH
FROM kpi.`kpi_daily_4g` a
WHERE a.tanggal BETWEEN "'''+date_1+'''" AND "'''+date_2+'''" '''+query_siteid+'''
GROUP BY a.`tanggal`,a.`siteid`;
'''

name_file = '4guser '+date_1+' to '+date_2+'.csv'
df = pd.read_sql(query, conn)

df.to_csv(
    '''F:/KY/prod/download/'''+name_file, index=False)

try:
    update.message.bot.sendDocument(update.message.chat.id, open(
        'F:/KY/prod/download/'+name_file, 'rb'))
except:
    os.environ["https_proxy"] = "https://10.59.66.1:8080"
    os.system("telegram-send --file F:/KY/prod/download/"+name_file+"")
