from bs4 import BeautifulSoup
from urllib.request import urlopen
import pandas as pd
import numpy as np
import datetime



url="https://www.agribank.com.vn/vn/lai-suat"

soup = BeautifulSoup(urlopen(url).read())
rows =[]
for tr in soup.findAll('table')[0].findAll('tr'):
    print (tr)
    rows.append([td.get_text(strip=True) for td in tr.select('th, td')])

    
df=pd.DataFrame(rows)

new_header = df.iloc[0]
df = df[1:]
df.columns='KY_HAN','VND','USD','EUR'
df['UPDATE_DATE'] = datetime.date.today()
df['PRIMARY_KEY'] = df['KY_HAN']+df['VND']+df['UPDATE_DATE'].astype(str)+'AGRIBANK'
df['BANK_NAME'] = 'AGRIBANK'

df.to_csv('out/tables/LAI_SUAT_AGRIBANK.csv', index = False,encoding='utf-8')
