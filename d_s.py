from __future__ import print_function
import pickle
import os.path
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from googleapiclient.http import MediaFileUpload
from googleapiclient.errors import HttpError
import sqlite3
import logging
from time import sleep

PATH = os.getcwd() + "/settings/"
SCOPES = ['https://www.googleapis.com/auth/drive']
SCOPES_SHEETS = ['https://www.googleapis.com/auth/spreadsheets']
CATEGORY = ["Видеоотчёт по одежде", "Фотоотчёт авто"]


def img_upload_drive(chat_id, id_folder, pics, drive_service):
    for img in pics:
        name = img.split('/')[-1]
        logging.info(str(chat_id[0]) + ' Копируем файл ' + str(name))
        file_path = img
        file_metadata = {
            'name': name,
            'parents': [id_folder]
        }
        media = MediaFileUpload(file_path, resumable=True)
        try:
            drive_service.files().create(body=file_metadata, media_body=media, fields='id').execute()
        except HttpError as err:
            logging.info(str(chat_id[0]) + err)
            if err.resp.status in [403, 500, 503]:
                sleep(5)
                try:
                    drive_service.files().create(body=file_metadata, media_body=media, fields='id').execute()
                except HttpError as err:
                    logging.info(str(chat_id[0]) + err + ' повторно, пропускаем ' + img)
                    continue


def sql_requests(sql):
    try:
        conn = sqlite3.connect(PATH + "bot_sql.db")
        cursor = conn.cursor()
        cursor.execute(sql)
        if "SELECT" in sql:
            return cursor.fetchall()
        elif "UPDATE" in sql:
            conn.commit()
        elif "INSERT" in sql:
            conn.commit()
        elif "DELETE" in sql:
            conn.commit()
    except Exception as e:
        logging.info('sql_requests ' + str(e))


def color_raw(sheets_service, SAMPLE_SPREADSHEET_ID, sheetId, i):
    sheets_service.spreadsheets().batchUpdate(spreadsheetId=SAMPLE_SPREADSHEET_ID,
                                              body=
                                              {
                                                  "requests":
                                                      [
                                                          {
                                                              "repeatCell":
                                                                  {
                                                                      "cell":
                                                                          {
                                                                              "userEnteredFormat":
                                                                                  {
                                                                                      # "horizontalAlignment": 'CENTER',
                                                                                      "backgroundColor": {
                                                                                          "red": 0.74,
                                                                                          "green": 0.93,
                                                                                          "blue": 0.71,
                                                                                          "alpha": 1.0
                                                                                      }
                                                                                      # "textFormat":
                                                                                      #  {
                                                                                      #    "bold": False,
                                                                                      #    "fontSize": 10
                                                                                      #  }
                                                                                  }
                                                                          },
                                                                      "range":
                                                                          {
                                                                              "sheetId": sheetId,
                                                                              "startRowIndex": i,
                                                                              "endRowIndex": i + 1,
                                                                              "startColumnIndex": 0,
                                                                              "endColumnIndex": 7
                                                                          },
                                                                      "fields": "userEnteredFormat"
                                                                  }
                                                          }
                                                      ]
                                              }).execute()


def main(chat_id, report_type):
    f = open(PATH + 'settings.txt', encoding='utf-8')
    f_line = f.readlines()
    f.close()
    creds = None
    if os.path.exists(PATH + 'token.pickle'):
        with open(PATH + 'token.pickle', 'rb') as token:
            creds = pickle.load(token)
    # If there are no (valid) credentials available, let the user log in.
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                PATH + 'credentials.json', SCOPES)
            creds = flow.run_local_server()
        # Save the credentials for the next run
        with open(PATH + 'token.pickle', 'wb') as token:
            pickle.dump(creds, token)

    drive_service = build('drive', 'v3', credentials=creds, cache_discovery=False)

    creds = None
    if os.path.exists(PATH + 'token.pickle'):
        with open(PATH + 'token.pickle', 'rb') as token:
            creds = pickle.load(token)
    # If there are no (valid) credentials available, let the user log in.
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                PATH + 'credentials.json', SCOPES_SHEETS)
            creds = flow.run_local_server()
        # Save the credentials for the next run
        with open(PATH + 'token.pickle', 'wb') as token:
            pickle.dump(creds, token)

    sheets_service = build('sheets', 'v4', credentials=creds, cache_discovery=False)
    chat_id = [str(chat_id)]
    for chat_id_user in chat_id:
        if report_type == CATEGORY[0]:
            table = 'clothes'
            parentID = (f_line[4].split('=')[1]).strip('\n')
            SAMPLE_SPREADSHEET_ID = (f_line[3].split('=')[1]).strip('\n')
            sql = 'SELECT employees.chat_id, employees.city, employees.fio, clothes.data, ' \
                  'clothes.data_now, clothes.list_pic, clothes.last_url ' \
                  'FROM employees ' \
                  'JOIN clothes ON clothes.chat_id = employees.chat_id ' \
                  'WHERE employees.chat_id ="' + str(chat_id_user) + '"'
            alll = sql_requests(sql)
            id_user = alll[0][0]
            fio = alll[0][2]
            date_user = alll[0][3]
            pices = alll[0][5].split(',')
            nm_folder = str(alll[0][1]) + '-' + alll[0][2]
            logging.info(str(chat_id_user) + ' ' + nm_folder + ' для clothes')
        elif report_type == CATEGORY[1]:
            table = 'car'
            parentID = (f_line[2].split('=')[1]).strip('\n')
            SAMPLE_SPREADSHEET_ID = (f_line[1].split('=')[1]).strip('\n')
            sql = 'SELECT employees.chat_id, employees.city, car.model, car.car_number, employees.fio, car.data, ' \
                  'car.data_now, car.list_pic, car.last_url, car.num_STS ' \
                  'FROM employees ' \
                  'JOIN car ON car.chat_id = employees.chat_id ' \
                  'WHERE employees.chat_id ="' + str(chat_id_user) + '"'
            alll = sql_requests(sql)
            id_user = alll[0][0]
            date_user = alll[0][5]
            number_car = alll[0][3]
            pices = alll[0][7].split(',')
            nm_folder = str(alll[0][1]) + '-' + alll[0][2] + '-' + alll[0][3] + '-' + alll[0][4]
            logging.info(str(chat_id_user) + ' ' + nm_folder + ' для car')
        pics = []
        for pic in pices:
            pics.append(pic.strip('[').strip(']').strip(" ").strip("'"))
        query = "'" + parentID + "'" + " in parents"
        results = drive_service.files().list(q=query, pageSize=200, fields="nextPageToken, files(id, name)").execute()
        items = results.get('files', [])
        logging.info(str(chat_id_user) + ' Кол-во папок id на drive ' + str(len(items)))
        list_name_items = []

        for item in items:
            name_folder_drive = item['name']
            id_data_folder = item['id']
            list_name_items.append(name_folder_drive)

            # Если папка с датой есть то сюда:
            if str(date_user) == str(name_folder_drive):
                folder_name_user_id = item['id']
                logging.info(str(chat_id_user) + ' Нашел папку в корне ' + str(name_folder_drive))
                # Проверяем есть ли конечная папка в папке с датой
                query = "'" + str(folder_name_user_id) + "'" + " in parents"
                results = drive_service.files().list(q=query, pageSize=200,
                                                     fields="nextPageToken, files(id, name)").execute()
                all_korn_folders = results.get('files', [])
                list_end_folders = []
                # В папке с датой пусто
                if len(all_korn_folders) == 0:
                    logging.info(str(chat_id_user) + ' ' + 'Нет корневой папки в ' + str(name_folder_drive))
                    file_metadata = {
                        'name': nm_folder,
                        'mimeType': 'application/vnd.google-apps.folder',
                        'parents': [id_data_folder]
                    }
                    file = drive_service.files().create(body=file_metadata, fields='id').execute()
                    id_end_folder = file.get('id')
                    logging.info(str(chat_id_user) + ' Создали папку 158 ' + str(nm_folder))
                    list_end_folders.append(nm_folder)
                    url = 'https://drive.google.com/open?id=' + id_end_folder
                    logging.info(str(chat_id_user) + ' 161 Запускаю функцию загрузки фото на drive')
                    img_upload_drive(chat_id, id_end_folder, pics, drive_service)
                    logging.info(str(chat_id_user) + ' 163 Закончил')
                elif len(all_korn_folders) > 0:
                    for korn_folder in all_korn_folders:
                        name_folder_end = korn_folder['name']
                        list_end_folders.append(name_folder_end)
                        if report_type == CATEGORY[0]:
                            if fio in name_folder_end:
                                folder_id_last_date = korn_folder['id']
                                logging.info(str(chat_id_user) + ' Папка есть ' + str(korn_folder['name']))
                                url = 'https://drive.google.com/open?id=' + folder_id_last_date
                                # копируем файлы в конченую папку
                                logging.info(str(chat_id_user) + ' 186 Запускаю функцию загрузки на drive')
                                img_upload_drive(chat_id, folder_id_last_date, pics, drive_service)
                                logging.info(str(chat_id_user) + ' 188 Закончил')
                                logging.info(str(chat_id_user) + ' ' + fio)
                        elif report_type == CATEGORY[1]:
                            if number_car in name_folder_end:
                                folder_id_last_date = korn_folder['id']
                                logging.info(str(chat_id_user) + ' Папка есть ' + str(korn_folder['name']))
                                url = 'https://drive.google.com/open?id=' + folder_id_last_date
                                # копируем файлы в конченую папку
                                logging.info(str(chat_id_user) + ' 196 Запускаю функцию загрузки на drive')
                                img_upload_drive(chat_id, folder_id_last_date, pics, drive_service)
                                logging.info(str(chat_id_user) + ' 198 Закончил')
                                logging.info(str(chat_id_user) + ' ' + number_car)
                if report_type == CATEGORY[0]:
                    if not fio in str(list_end_folders):
                        logging.info(str(chat_id_user) + ' ' + 'Нет корневой папки в ' + str(name_folder_drive))
                        file_metadata = {
                            'name': nm_folder,
                            'mimeType': 'application/vnd.google-apps.folder',
                            'parents': [id_data_folder]
                        }
                        file = drive_service.files().create(body=file_metadata, fields='id').execute()
                        id_end_folder = file.get('id')
                        logging.info(str(chat_id_user) + ' Создали папку 211 ' + str(nm_folder))
                        url = 'https://drive.google.com/open?id=' + id_end_folder
                        logging.info(str(chat_id_user) + ' 213 Запускаю функцию загрузки на drive')
                        img_upload_drive(chat_id, id_end_folder, pics, drive_service)
                        logging.info(str(chat_id_user) + ' 215 Закончил')
                elif report_type == CATEGORY[1]:
                    if not number_car in str(list_end_folders):
                        logging.info(str(chat_id_user) + ' ' + 'Нет корневой папки в ' + str(name_folder_drive))
                        file_metadata = {
                            'name': nm_folder,
                            'mimeType': 'application/vnd.google-apps.folder',
                            'parents': [id_data_folder]
                        }
                        file = drive_service.files().create(body=file_metadata, fields='id').execute()
                        id_end_folder = file.get('id')
                        logging.info(str(chat_id_user) + ' Создали папку 226 ' + str(nm_folder))
                        url = 'https://drive.google.com/open?id=' + id_end_folder
                        logging.info(str(chat_id_user) + ' 228 Запускаю функцию загрузки на drive')
                        img_upload_drive(chat_id, id_end_folder, pics, drive_service)
                        logging.info(str(chat_id_user) + ' 230 Закончил')
        if not str(date_user) in str(list_name_items):
            logging.info(str(chat_id_user) + ' ' + 'Нет ' + str(date_user))
            # Создаем папки с датой в корневой папке
            file_metadata = {
                'name': date_user,
                'mimeType': 'application/vnd.google-apps.folder',
                'parents': [parentID]
            }
            file = drive_service.files().create(body=file_metadata, fields='id').execute()
            folder_name_user_id = file.get('id')

            # Создаем папки конечные
            folder_id_user_id = folder_name_user_id
            file_metadata = {
                'name': nm_folder,
                'mimeType': 'application/vnd.google-apps.folder',
                'parents': [folder_id_user_id]
            }
            file = drive_service.files().create(body=file_metadata, fields='id').execute()
            folder_name_date_user_id = file.get('id')
            url = 'https://drive.google.com/open?id=' + folder_name_date_user_id
            # копируем файлы в папку конечную
            logging.info(str(chat_id_user) + ' 255 Запускаю функцию загрузки на drive')
            img_upload_drive(chat_id, folder_name_date_user_id, pics, drive_service)
            logging.info(str(chat_id_user) + ' 257 Закончил')

            # Создаем лист с текущим месецем
            batch_update_spreadsheet_request_body = {
                'requests': [
                    {
                        "addSheet": {
                            "properties": {
                                "title": "" + date_user + "",
                                "gridProperties": {
                                    "rowCount": 200,
                                    "columnCount": 20
                                },
                            }
                        }
                    }
                ],  # TODO: Update placeholder value.
            }

            request = sheets_service.spreadsheets().batchUpdate(spreadsheetId=SAMPLE_SPREADSHEET_ID,
                                                                body=batch_update_spreadsheet_request_body)
            try:
                request.execute()
                logging.info(str(chat_id_user) + 'Создан лист ' + str(date_user))
            except Exception as e:
                logging.info(str(chat_id_user) + str(e))
            # Заполняем заголовки
            try:
                if report_type == CATEGORY[0]:
                    values_title_sh = [['Дата фотоотчёта'], ['Город'], ['ФИО'], ['Ссылка на актуальную папку']]
                    sql = 'SELECT employees.chat_id, employees.city, employees.fio, clothes.data, clothes.data_now' \
                          ', clothes.list_pic, clothes.last_url ' \
                          'FROM employees ' \
                          'JOIN clothes ON clothes.chat_id = employees.chat_id'
                    line_sql = sql_requests(sql)
                    count_id = len(line_sql)
                    logging.info(str(chat_id_user) + ' Кол-во записей в базе clothes ' + str(count_id))

                    range_a = '!A1:D1'
                    key = ':D'
                elif report_type == CATEGORY[1]:
                    values_title_sh = [['Дата фотоотчёта'], ['Город'], ['Модель'], ['Гос. номер'], ['Номер СТС'],
                                       ['ФИО'],
                                       ['Ссылка на актуальную папку']]

                    sql = 'SELECT employees.chat_id, car_responsible.city, car.model, car.car_number, employees.fio, ' \
                          'car.data, car.data_now, car.list_pic, car.last_url, car.num_STS ' \
                          'FROM car ' \
                          'JOIN car_responsible ON car.car_number = car_responsible.car_number ' \
                          'LEFT OUTER JOIN employees ON car.chat_id = employees.chat_id'
                    line_sql = sql_requests(sql)
                    count_id = len(line_sql)
                    logging.info(str(chat_id_user) + ' Кол-во записей в базе car ' + str(count_id))

                    range_a = '!A1:G1'
                    key = ':G'
                sheets_service.spreadsheets().values().batchUpdate(spreadsheetId=SAMPLE_SPREADSHEET_ID, body={
                    "valueInputOption": "USER_ENTERED",
                    "data": [
                        {"range": "" + date_user + range_a,
                         "majorDimension": "COLUMNS",
                         # сначала заполнять ряды, затем столбцы (т.е. самые внутренние списки в values - это ряды)
                         "values": values_title_sh}
                    ]
                }).execute()

                # заполняем новый лист базовыми значения из базы данны(Город, Модель, Гос. номер, ФИО)
                j = 2
                for line in line_sql:
                    if report_type == CATEGORY[0]:
                        val_list = [[line[4]], [line[1]], [line[2]], [line[6]]]
                    elif report_type == CATEGORY[1]:
                        val_list = [[line[6]], [line[1]], [line[2]], [line[3]], [line[9]], [line[4]], [line[8]]]
                    sheets_service.spreadsheets().values().batchUpdate(spreadsheetId=SAMPLE_SPREADSHEET_ID,
                                                                       body={
                                                                           "valueInputOption": "USER_ENTERED",
                                                                           "data": [
                                                                               {
                                                                                   "range": "" + date_user + "!A" + str(
                                                                                       j) + key + str(j),
                                                                                   "majorDimension": "COLUMNS",
                                                                                   "values": val_list}
                                                                           ]
                                                                       }).execute()
                    sleep(1)
                    if report_type == CATEGORY[0]:
                        logging.info(str(chat_id_user) + ' ' + line[1] + ' ' + line[2])
                        j += 1

                    elif report_type == CATEGORY[1]:
                        if line[4] is None:
                            logging.info(str(chat_id_user) + ' ' + line[1] + ' ' + line[2] + ' ' + line[3])
                            j += 1
                        else:
                            logging.info(str(chat_id_user) + ' ' + line[1] + ' ' + line[2] + ' ' + line[3] + ' ' + line[4])
                            j += 1
                logging.info(
                    str(chat_id_user) + 'Заполняем заголовок листа ' + str(date_user) + ' и основые данные из базы данных')
                spreadsheet = sheets_service.spreadsheets().get(spreadsheetId=SAMPLE_SPREADSHEET_ID).execute()
                sheetList = spreadsheet.get('sheets')
                for sheet in sheetList:
                    if sheet['properties']['title'] == date_user:
                        sheetId = sheet['properties']['sheetId']
                        break
                sheets_service.spreadsheets().batchUpdate(spreadsheetId=SAMPLE_SPREADSHEET_ID,
                                                          body=
                                                          {
                                                              "requests":
                                                                  [
                                                                      {
                                                                          "sortRange":
                                                                              {
                                                                                  "range":
                                                                                      {
                                                                                          "sheetId": sheetId,
                                                                                          "startRowIndex": 1,
                                                                                          "endRowIndex": 200,
                                                                                          "startColumnIndex": 0,
                                                                                          "endColumnIndex": 7
                                                                                      },
                                                                                  "sortSpecs": [
                                                                                      {
                                                                                          "dimensionIndex": 1,
                                                                                          "sortOrder": "ASCENDING"
                                                                                      },
                                                                                      {
                                                                                          "dimensionIndex": 2,
                                                                                          "sortOrder": "ASCENDING"
                                                                                      },
                                                                                      {
                                                                                          "dimensionIndex": 3,
                                                                                          "sortOrder": "ASCENDING"
                                                                                      }
                                                                                  ]
                                                                              }
                                                                      }
                                                                  ]
                                                          }).execute()
            except Exception as e:
                logging.info(str(chat_id_user) + str(e))
        # заносим в sql ссылку на папку гугл диска
        sql = """
        UPDATE """ + table + """ 
        SET last_url = \"""" + str(url) + """\"
        WHERE chat_id = '""" + str(id_user) + """'
            """
        sql_requests(sql)
        spreadsheet = sheets_service.spreadsheets().get(spreadsheetId=SAMPLE_SPREADSHEET_ID).execute()
        sheetList = spreadsheet.get('sheets')
        for sheet in sheetList:
            if sheet['properties']['title'] == date_user:
                sheetId = sheet['properties']['sheetId']
                break
        try:
            if report_type == CATEGORY[0]:
                range_ = date_user + '!C1:200'
            elif report_type == CATEGORY[1]:
                range_ = date_user + '!D1:200'
            request = sheets_service.spreadsheets().values().get(spreadsheetId=SAMPLE_SPREADSHEET_ID, range=range_)
            response = request.execute()
            sleep(1)
            k = len(response["values"])
            r = response["values"]
            in_table = False
            for i in range(1, k):
                try:
                    if report_type == CATEGORY[0]:
                        if r[i][0] == fio:
                            logging.info(
                                str(chat_id[0]) + ' Нашел ФИО ' + str(fio) + ' в табилце ' + str(date_user))
                            values = [[alll[0][4]], [alll[0][1]], [alll[0][2]], [url]]
                            color_raw(sheets_service, SAMPLE_SPREADSHEET_ID, sheetId, i)
                            logging.info(str(chat_id_user) + ' Цвет строки ' + str(i + 1) + ' закрашен')
                            range_ = date_user + "!A" + str(i + 1) + ":D" + str(i + 1)
                            sheets_service.spreadsheets().values().batchUpdate(
                                spreadsheetId=SAMPLE_SPREADSHEET_ID, body={
                                    "valueInputOption": "USER_ENTERED",
                                    "data": [
                                        {"range": range_,
                                         "majorDimension": "COLUMNS",
                                         "values": values}
                                    ]
                                }).execute()
                            logging.info(str(chat_id_user) + ' инф в строку ' + str(i + 1) + ' занесена ')
                            logging.info(str(chat_id_user) + ' Обновлены данные в листе ' + str(date_user))
                            in_table = True
                            break
                    elif report_type == CATEGORY[1]:
                        if r[i][0] == number_car:
                            logging.info(
                                str(chat_id[0]) + ' Нашел номер ' + str(number_car) + ' в табилце ' + str(date_user))
                            values = [[alll[0][6]], [alll[0][1]], [alll[0][2]], [alll[0][3]], [alll[0][9]],
                                      [alll[0][4]], [url]]
                            color_raw(sheets_service, SAMPLE_SPREADSHEET_ID, sheetId, i)
                            logging.info(str(chat_id_user) + ' Цвет строки ' + str(i + 1) + ' закрашен')
                            sheets_service.spreadsheets().values().batchUpdate(
                                spreadsheetId=SAMPLE_SPREADSHEET_ID, body={
                                    "valueInputOption": "USER_ENTERED",
                                    "data": [
                                        {"range": "" + date_user + "!A" + str(i + 1) + ":G" + str(i + 1),
                                         "majorDimension": "COLUMNS",
                                         # сначала заполнять ряды, затем столбцы (т.е. самые внутренние списки в values - это ряды)
                                         "values": values}
                                    ]
                                }).execute()
                            logging.info(str(chat_id_user) + ' инф в строку ' + str(i + 1) + ' занесена ')
                            logging.info(str(chat_id_user) + ' Обновлены данные в листе ' + str(date_user))
                            in_table = True
                            break
                except KeyError:
                    logging.info(str(chat_id_user) + ' Пустая ячейка ' + range_)
                    break
            if in_table is False:
                if report_type == CATEGORY[0]:
                    values = [[alll[0][4]], [alll[0][1]], [alll[0][2]], [url]]
                elif report_type == CATEGORY[1]:
                    values = [[alll[0][6]], [alll[0][1]], [alll[0][2]], [alll[0][3]], [alll[0][9]], [alll[0][4]], [url]]
                color_raw(sheets_service, SAMPLE_SPREADSHEET_ID, sheetId, k)
                sheets_service.spreadsheets().values().batchUpdate(spreadsheetId=SAMPLE_SPREADSHEET_ID, body={
                    "valueInputOption": "USER_ENTERED",
                    "data": [
                        {"range": "" + date_user + "!A" + str(k + 1) + ":J" + str(k + 1),
                         "majorDimension": "COLUMNS",
                         # сначала заполнять ряды, затем столбцы (т.е. самые внутренние списки в values - это ряды)
                         "values": values}
                    ]
                }).execute()
                logging.info(str(chat_id_user) + ' Записаны новые данные в лист ' + str(date_user))
        except Exception as e:
            logging.info(str(chat_id_user) + str(e))
