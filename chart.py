import matplotlib.pyplot as plt
import mysql.connector
from datetime import datetime, timedelta
import os

date_x = datetime.today() - timedelta(days=1)
date_y = date_x - timedelta(days=1*30)
date_x_ = date_x.strftime("%Y-%m-%d")
date_y_ = date_y.strftime("%Y-%m-%d")
date_label = date_x.strftime("%d %B %Y")

# DB CONFIG
mydb = mysql.connector.connect(
    host="10.47.150.144",
    user="sqaviewjbr",
    password="5qAv13wJbr@2019#",
    database="kpi"
)

mycursor = mydb.cursor()

query = '''SELECT `tanggal`,
ROUND(SUM(`payload_2g_total_mbit`)/(8*1024*1024)) PY_2G_TB,
ROUND(SUM(`payload_3g_total_mbit`)/(8*1024*1024)) PY_3G_TB,
ROUND(SUM(`payload_4g_total_mbit`)/(8*1024*1024)) PY_4G_TB,
ROUND(SUM(`payload_total_mbit`)/(8*1024*1024)) PY_TOTAL_TB
FROM `productivity_daily_region`
WHERE tanggal BETWEEN "'''+date_y_+'''" AND "'''+date_x_+'''" AND region = 'JABAR'
GROUP BY `tanggal`;
'''

mycursor.execute(query)

Tanggal = []
Py_2g = []
Py_3g = []
Py_4g = []
Py_total = []
 
for i in mycursor:
    Tanggal.append(i[0])
    Py_2g.append(i[1])
    Py_3g.append(i[2])
    Py_4g.append(i[3])
    Py_total.append(i[4])

my_dpi = 120

plt.figure(figsize=(21,5), dpi=my_dpi)

plt.plot(Tanggal, Py_total, label='Payload Total', marker='o')
plt.plot(Tanggal, Py_4g, label='Payload 4G', marker='o')
plt.plot(Tanggal, Py_3g, label='Payload 3G', marker='o')
plt.plot(Tanggal, Py_2g, label='Payload 2G', marker='o')

plt.legend(loc="best")
plt.xlabel("DATE")
plt.ylabel("PAYLOAD TB")
plt.title("TREND PAYLOAD JABAR "+date_label)
plt.savefig('F:/KY/prod/download/chart.jpg', dpi=my_dpi, bbox_inches='tight')

try:
    update.message.bot.send_photo(update.message.chat.id, open('F:/KY/prod/download/chart.jpg','rb'))
except:
    os.environ["https_proxy"] = "https://10.59.66.1:8080"
    os.system("telegram-send --image F:/KY/prod/download/chart.jpg")