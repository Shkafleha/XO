from google.colab import drive
drive.mount('/content/drive/')

import apiclient.discovery
from oauth2client.service_account import ServiceAccountCredentials
import httplib2

import pandas as pd


"""Загрузка данных"""

#  имя скаченного файла с закрытым ключом
CREDENTIALS_FILE = '/content/drive/My Drive/Colab Notebooks/XO/xo.json'
credentials = ServiceAccountCredentials.from_json_keyfile_name(CREDENTIALS_FILE, ['https://www.googleapis.com/auth/spreadsheets',                                        'https://www.googleapis.com/auth/drive'])
httpAuth = credentials.authorize(httplib2.Http())
service = apiclient.discovery.build('sheets', 'v4', http = httpAuth)
spreadsheetId = "1Ycg7zTxds9DZnDvTrFcyNNKuTUxg6Yy6WF0a8Wc02WQ"

# Загружаем транзакции, преводим данные к нужным типам
range_name = 'transactions'
transactions_raw = service.spreadsheets().values().batchGet(spreadsheetId = spreadsheetId, 
                                     ranges = range_name, 
                                     valueRenderOption = 'FORMATTED_VALUE',  
                                     dateTimeRenderOption = 'FORMATTED_STRING').execute() 
transactions = pd.DataFrame(transactions_raw['valueRanges'][0]['values'])
transactions.columns = transactions.iloc[0]
transactions = transactions.iloc[1:]
transactions['created_at'] = pd.to_datetime(transactions['created_at'], errors = 'coerce')
transactions['m_real_amount'] = transactions['m_real_amount'].astype('int') 

# Загружаем клиентов, преводим данные к нужным типам
range_name = 'clients'
clients_raw = service.spreadsheets().values().batchGet(spreadsheetId = spreadsheetId, 
                                     ranges = range_name, 
                                     valueRenderOption = 'FORMATTED_VALUE',  
                                     dateTimeRenderOption = 'FORMATTED_STRING').execute() 
clients = pd.DataFrame(clients_raw['valueRanges'][0]['values'])
clients.columns = clients.iloc[0]
clients = clients.iloc[1:]
clients['created_at'] = pd.to_datetime(clients['created_at'], errors = 'coerce')

# Загружаем менеджеров, преводим данные к нужным типам
range_name = 'managers'
manager_raw = service.spreadsheets().values().batchGet(spreadsheetId = spreadsheetId, 
                                     ranges = range_name, 
                                     valueRenderOption = 'FORMATTED_VALUE',  
                                     dateTimeRenderOption = 'FORMATTED_STRING').execute() 
manager = pd.DataFrame(manager_raw['valueRanges'][0]['values'])
manager.columns = manager.iloc[0]
manager = manager.iloc[1:]
manager = manager.set_index('manager_id')

# Загружаем заявки, преводим данные к нужным типам
range_name = 'leads'
leads_raw = service.spreadsheets().values().batchGet(spreadsheetId = spreadsheetId, 
                                     ranges = range_name, 
                                     valueRenderOption = 'FORMATTED_VALUE',  
                                     dateTimeRenderOption = 'FORMATTED_STRING').execute() 
leads = pd.DataFrame(leads_raw['valueRanges'][0]['values'])
leads.columns = leads.iloc[0]
leads = leads.iloc[1:]
leads['created_at'] = pd.to_datetime(leads['created_at'], errors = 'coerce')
# сортируем заявки по дате
leads = leads.sort_values(by=['created_at'])


"""Создаём столбцы с нужными нам данными"""

# Выбираем спамовые заявки
leads['spam'] = (leads['l_client_id'] == '00000000-0000-0000-0000-000000000000')
leads['no_manager'] = (leads['l_manager_id'] == '00000000-0000-0000-0000-000000000000')

# Выбираем тех кто купил
def bought(row):
    transactions_date_range =transactions[
            (transactions['created_at'] >= row['created_at']) &
            (transactions['created_at'] <= row['created_at'] + pd.Timedelta("7 days"))
            ]
    if row['l_client_id'] in transactions_date_range['l_client_id'].values:
        return True
    else:
        return False
leads['bought'] = leads.apply(bought, axis=1)

# Добавляем новых клиентов
leads['new'] = ~leads['l_client_id'].duplicated()
leads['new_bought'] = leads['bought'] & leads['new']

# Добавляем менеджеров в таблицу
def get_manager(row):
  if row['l_manager_id'] in manager.index:
      return manager.loc[row['l_manager_id']][['d_manager', 'd_club']].values
  else:
      return ['no_manager', 'no_manager']

leads[['manager','club']] = pd.DataFrame.from_records(list(leads.apply(get_manager, axis = 1)))

# Добавляем сумму покупок для новых клиентов
def get_manager(row):
    if row['l_client_id'] in transactions['l_client_id'].values:
        return sum(transactions[transactions['l_client_id'] == row['l_client_id']]['m_real_amount'].values)
    else:
        return 0
leads['new_amount'] = leads.apply(get_manager, axis = 1)


"""Агрегация количества заказов по дням"""

leads['created_at_day'] = leads['created_at'].dt.date
pivot_leads = leads.pivot_table(
    index=['created_at_day', 'd_utm_source', 'club', 'manager'], values=['lead_id'], aggfunc='count')
pivot_main = leads.pivot_table(
    index=['created_at_day', 'd_utm_source', 'club', 'manager'],
    values = ['new', 'new_amount',	'new_bought',	'no_manager',	'spam'],
    aggfunc='sum')
pivot_total = pivot_leads.join(pivot_main, how='left', lsuffix='_left')


"""Подготовка данных для записи"""

pivot_total = pivot_total.reset_index()
pivot_total['created_at_day'] = pivot_total['created_at_day'].astype('str')


"""Сохранение в Google Docs"""

CREDENTIALS_FILE = '/content/drive/My Drive/Colab Notebooks/XO/xo.json'
credentials = ServiceAccountCredentials.from_json_keyfile_name(CREDENTIALS_FILE, ['https://www.googleapis.com/auth/spreadsheets',                                        'https://www.googleapis.com/auth/drive'])
httpAuth = credentials.authorize(httplib2.Http())
service = apiclient.discovery.build('sheets', 'v4', http = httpAuth)
spreadsheetId = "1oF-8ZZtr2Av5aaVrFcVshwqkNtlXPCH9z5_hXnoUlnM"
range_name = 'Легенда'
list_title = 'result'
row = 1
cell = 'A'+str(row)
gruz = pivot_total.values.tolist()


service.spreadsheets().values().batchUpdate(spreadsheetId = spreadsheetId, 
		body = {
		"valueInputOption": "USER_ENTERED",
		"data": [
			{"range": list_title + "!" + cell,
			 "majorDimension": "ROWS",
			 "values": gruz}

		]
}).execute()

