from polygon import RESTClient
import pandas as pd
import datetime, time

import gspread

import pygsheets

import numpy as np
from calcbsimpvol import calcbsimpvol


#open the google spreadsheet 




#access e-mail address
acc_email="sheetreader@python-link-382709.iam.gserviceaccount.com"

# json 파일이 위치한 경로를 값으로 줘야 합니다.
json_file_path = "c:\gspy\gspy.json"
gc = gspread.service_account(json_file_path)
spreadsheet_url = "https://docs.google.com/spreadsheets/d/1M0LjBg2tPZprA-BIvsOZXjNPyK_gKm4pY4Ns93gvgJo/edit#gid=1093405521"
#sht = gc.open_by_url(spreadsheet_url)
#ws = sht.worksheet("gspy")
#ws_output = sht.worksheet("gspyo")


#using pygsheets
gc2 = pygsheets.authorize(service_file=json_file_path)
sht2 = gc2.open_by_url(spreadsheet_url)
ws2 = sht2.worksheet_by_title("gspy")
ws_output2 = sht2.worksheet_by_title("gspyo")


#ticker='COIN'
#expdate='240419'
#stprice='00245'
#candp='C'
#read cell
tkr_input = ws2.get_row(2)
ticker = tkr_input[0]
expdate = tkr_input[7]
stpriceC = tkr_input[3]
cdecimal = tkr_input[4]
stpriceP = tkr_input[5]
pdecimal = tkr_input[6]
st_date = tkr_input[1]
#ticker = ws.acell('A1').value
#expdate = ws.acell('A2').value
#stpriceC = ws.acell('A3').value
#stpriceP = ws.acell('A4').value
#st_date = ws.acell('A5').value
#candp = ws.acell('A4').value
#write cell
#ws.update([[1, 2], [3, 4]],'A2:B3')

api_key = "n5XzRnjNp98UzK4hJiu5WvhDgDYHUIGJ"


client = RESTClient(api_key)
#ticker='COIN'
#expdate='240419'


stpriceC=format(int(stpriceC),'05')
stpriceP=format(int(stpriceP),'05')

candp='C'
tkrstringC='O:'+ticker+expdate+candp+stpriceC+cdecimal

candp='P'
tkrstringP='O:'+ticker+expdate+candp+stpriceP+pdecimal


tday=datetime.datetime.today()
ddelta = datetime.timedelta(days=-14)
#st_date=(tday+ddelta).strftime('%Y-%m-%d')
#end_date=tday.strftime('%Y-%m-%d')
expdatef='20'+expdate[0:2]+'-'+expdate[2:4]+'-'+expdate[4:6]
end_date = expdatef

aggsAst = []
for a in client.list_aggs(
    ticker,
    1,
    "day",
    st_date,
    end_date,
    limit=50000,
):
    aggsAst.append(a)

aggsAst_df = pd.DataFrame(aggsAst)


aggsC = []
for a in client.list_aggs(
    tkrstringC,
    1,
    "day",
    st_date,
    end_date,
    limit=50000,
):
    aggsC.append(a)

aggsC_df = pd.DataFrame(aggsC)

aggsP = []
for a in client.list_aggs(
    tkrstringP,
    1,
    "day",
    st_date,
    end_date,
    limit=50000,
):
    aggsP.append(a)

epoch_time = aggsAst_df['timestamp']

date_time=pd.to_datetime(epoch_time,unit='ms').dt.strftime("%Y-%m-%d")

aggsAst_df['timestamp'] = date_time


aggsP_df = pd.DataFrame(aggsP)

epoch_time = aggsC_df['timestamp']

date_time=pd.to_datetime(epoch_time,unit='ms').dt.strftime("%Y-%m-%d")

aggsC_df['timestampC'] = date_time
aggsC_df['closeC'] = aggsC_df['close']

epoch_time = aggsP_df['timestamp']

date_time=pd.to_datetime(epoch_time,unit='ms').dt.strftime("%Y-%m-%d")

aggsP_df['timestampP'] = date_time
aggsP_df['closeP'] = aggsP_df['close']

<<<<<<< HEAD
ws2.clear(start ='C4',end='I13')
=======
ws2.clear(start ='C4',end='J13')
>>>>>>> 9fe7fab32acf6a12ef2f2cd9bcf1861d97c8431c
df_output=pd.concat([aggsAst_df[['timestamp','close']],aggsC_df[['timestampC','closeC']],aggsP_df[['timestampP','closeP']]],axis=1)

print(df_output)

ws2.set_dataframe(df_output, 'C3')

df_rownum = str(df_output.shape[0]+3)
#input("enter when ready")

df_gspy = ws2.get_as_df(has_header=True,index_column=None,start='B3',end='J'+df_rownum)
print(df_gspy)
#input("check astprice")
i=0
for astprice,ttm, optprice in zip(df_gspy['close'],df_gspy['ttm'],df_gspy['closeC']):
    if not isinstance(astprice,str):    
        S = np.asarray(astprice)        # ul asset price
        K = np.asarray([int(tkr_input[3])])        # strike price
        tau = np.asarray([ttm/365])    # Time to maturity
        r = np.asarray(0.045)        # interest rate
        q = np.asarray(0.00)        # Dividend Rate
        cp = np.asarray(0)       # option type call = 1 put = -1
        P= np.asarray([optprice])       # market Price
        imp_vol = calcbsimpvol(dict(cp=cp, P=P, S=S,K=K,tau=tau, r=r, q=q))
        iv_val = imp_vol[0][0]
        df_gspy['iv'].iloc[i]= round(iv_val,2)
        #print(imp_vol[0][0])
    i = i+1

df_iv = df_gspy[['iv']]
ws2.set_dataframe(df_iv,'I3')

