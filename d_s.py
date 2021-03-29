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
f = open(PATH + 'settings.txt', encoding='utf-8')
f_line = f.readlines()
f.close()
SCOPES = ['https://www.googleapis.com/auth/drive']
SCOPES_SHEETS = ['https://www.googleapis.com/auth/spreadsheets']
SAMPLE_SPREADSHEET_ID = (f_line[1].split('=')[1]).strip('\n')


def img_upload_drive(chat_id, id_folder, pics, drive_service):
    for img in pics:
        name = img.split('/')[-1]
        logging.info(str(chat_id) + 'Копируем файл ' + str(name))
        file_path = img
        file_metadata = {
            'name': name,
            'parents': [id_folder]
        }
        media = MediaFileUpload(file_path, resumable=True)
        try:
            r = drive_service.files().create(body=file_metadata, media_body=media, fields='id').execute()
        except HttpError as err:
            logging.info(str(chat_id) + err)
            print(str(chat_id) + err)
            if err.resp.status in [403, 500, 503]:
                sleep(5)
                try:
                    r = drive_service.files().create(body=file_metadata, media_body=media, fields='id').execute()
                except HttpError as err:
                    logging.info(str(chat_id) + err + ' повторно, пропускаем ' + img)
                    print(str(chat_id) + err + ' повторно, пропускаем ' + img)
                    continue


def main(chat_id):
    parentID = (f_line[2].split('=')[1]).strip('\n')
    # emails=['transfer1989@gmail.com','663301@vipceiling.ru']
    creds = None
    if os.path.exists('C:\\Bot\\token.pickle'):
        with open('C:\\Bot\\token.pickle', 'rb') as token:
            creds = pickle.load(token)
    # If there are no (valid) credentials available, let the user log in.
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                'C:\\Bot\\credentials.json', SCOPES)
            creds = flow.run_local_server()
        # Save the credentials for the next run
        with open('C:\\Bot\\token.pickle', 'wb') as token:
            pickle.dump(creds, token)

    drive_service = build('drive', 'v3', credentials=creds, cache_discovery=False)

    creds = None
    if os.path.exists('C:\\Bot\\token.pickle'):
        with open('C:\\Bot\\token.pickle', 'rb') as token:
            creds = pickle.load(token)
    # If there are no (valid) credentials available, let the user log in.
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                'C:\\Bot\\credentials.json', SCOPES_SHEETS)
            creds = flow.run_local_server()
        # Save the credentials for the next run
        with open('C:\\Bot\\token.pickle', 'wb') as token:
            pickle.dump(creds, token)

    sheets_service = build('sheets', 'v4', credentials=creds, cache_discovery=False)

    chat_id = [str(chat_id)]
    print(chat_id)
    logging.info(str(chat_id))
    path_sql = PATH + "bot_sql.db"
    conn = sqlite3.connect(path_sql)  # или :memory: чтобы сохранить в RAM
    cursor = conn.cursor()
    sql = "SELECT * FROM car WHERE chat_id=?"
    for chat_id_user in chat_id:
        cursor.execute(sql, [chat_id_user])
        alll = cursor.fetchall()
        id_user = alll[0][0]
        date_user = alll[0][5]
        number_car = alll[0][3]
        pices = alll[0][7].split(',')
        last_url = alll[0][8]
        # nm_folder=str(alll[0][1])+'-'+alll[0][2]+'-'+alll[0][3]+'-'+str(alll[0][5])+'-'+str(alll[0][6])+'-'+alll[0][4]#+'-'+str(alll[0][0])
        nm_folder = str(alll[0][1]) + '-' + alll[0][2] + '-' + alll[0][3] + '-' + alll[0][4]  # +'-'+str(alll[0][0])
        # print(nm_folder)
        logging.info(str(chat_id) + ' ' + nm_folder)
        # print(pices)
        pics = []
        for pic in pices:
            pics.append(pic.strip('[').strip(']').strip(" ").strip("'"))
        # print(pics)
        # print([x for x in alll[0]])

        query = "'" + parentID + "'" + " in parents"
        results = drive_service.files().list(q=query, pageSize=200, fields="nextPageToken, files(id, name)").execute()
        items = results.get('files', [])
        logging.info(str(chat_id) + ' ' + 'Кол-во папок id на drive ' + str(len(items)))
        list_name_items = []

        for item in items:
            name_folder_drive = item['name']
            id_data_folder = item['id']
            list_name_items.append(name_folder_drive)

            # Если папка с датой есть то сюда:
            if str(date_user) == str(name_folder_drive):
                folder_name_user_id = item['id']
                logging.info(str(chat_id) + ' ' + 'Нашел папку в корне ' + str(name_folder_drive))
                print(str(chat_id) + ' ' + 'Нашел папку в корне ' + str(name_folder_drive))
                # Проверяем есть ли конечная папка в папке с датой
                query = "'" + str(folder_name_user_id) + "'" + " in parents"
                results = drive_service.files().list(q=query, pageSize=200,
                                                     fields="nextPageToken, files(id, name)").execute()
                all_korn_folders = results.get('files', [])
                print(all_korn_folders)
                logging.info(all_korn_folders)
                list_end_folders = []
                # В папке с датой пусто
                if len(all_korn_folders) == 0:
                    logging.info(str(chat_id) + ' ' + 'Нет корневой папки в ' + str(name_folder_drive))
                    file_metadata = {
                        'name': nm_folder,
                        'mimeType': 'application/vnd.google-apps.folder',
                        'parents': [id_data_folder]
                    }
                    file = drive_service.files().create(body=file_metadata, fields='id').execute()
                    id_end_folder = file.get('id')
                    logging.info('Создали папку 113' + str(nm_folder))
                    list_end_folders.append(nm_folder)
                    url = 'https://drive.google.com/open?id=' + id_end_folder
                    print(url)
                    logging.info(str(chat_id) + ' 148 Запускаю функцию загрузки фото на drive')
                    img_upload_drive(chat_id, id_end_folder, pics, drive_service)
                    logging.info(str(chat_id) + ' 148 Закончил')
                    # for img in pics:
                    #     name = img.split('/')[-1]
                    #     #print(name)
                    #     logging.info(str(chat_id)+'Копируем файл '+str(name))
                    #     file_path = img
                    #     file_metadata = {
                    #                     'name': name,
                    #                     'parents': [id_end_folder]
                    #                 }
                    #     media = MediaFileUpload(file_path, resumable=True)
                    #     r = drive_service.files().create(body=file_metadata, media_body=media, fields='id').execute()

                elif len(all_korn_folders) > 0:

                    for korn_folder in all_korn_folders:
                        name_folder_end = korn_folder['name']
                        list_end_folders.append(name_folder_end)
                        if number_car in name_folder_end:
                            folder_id_last_date = korn_folder['id']
                            print('Папка есть ' + str(korn_folder['name']))
                            logging.info('Папка есть ' + str(korn_folder['name']))

                            url = 'https://drive.google.com/open?id=' + folder_id_last_date
                            print(url)
                            # копируем файлы в конченую папку
                            logging.info(str(chat_id) + ' 176 Запускаю функцию загрузки фото на drive')
                            img_upload_drive(chat_id, folder_id_last_date, pics, drive_service)
                            logging.info(str(chat_id) + ' 178 Закончил')
                            # for img in pics:
                            #     name = img.split('/')[-1]
                            #     #print(name)
                            #     logging.info(str(chat_id)+'Копируем файл '+str(name))
                            #     file_path = img
                            #     file_metadata = {
                            #                     'name': name,
                            #                     'parents': [folder_id_last_date]
                            #                 }
                            #     media = MediaFileUpload(file_path, resumable=True)
                            #     r = drive_service.files().create(body=file_metadata, media_body=media, fields='id').execute()
                print(number_car)
                logging.info(number_car)
                print(list_end_folders)
                logging.info(list_end_folders)
                if not number_car in str(list_end_folders):
                    logging.info(str(chat_id) + ' ' + 'Нет корневой папки в ' + str(name_folder_drive))
                    file_metadata = {
                        'name': nm_folder,
                        'mimeType': 'application/vnd.google-apps.folder',
                        'parents': [id_data_folder]
                    }
                    file = drive_service.files().create(body=file_metadata, fields='id').execute()
                    id_end_folder = file.get('id')
                    logging.info('Создали папку 174 ' + str(nm_folder))
                    url = 'https://drive.google.com/open?id=' + id_end_folder
                    print(url)
                    logging.info(str(chat_id) + ' 206 Запускаю функцию загрузки фото на drive')
                    img_upload_drive(chat_id, id_end_folder, pics, drive_service)
                    logging.info(str(chat_id) + ' 206 Закончил')
                    # for img in pics:
                    #     name = img.split('/')[-1]
                    #     #print(name)
                    #     logging.info(str(chat_id)+'Копируем файл '+str(name))
                    #     file_path = img
                    #     file_metadata = {
                    #                     'name': name,
                    #                     'parents': [id_end_folder]
                    #                 }
                    #     media = MediaFileUpload(file_path, resumable=True)
                    #     try:
                    #         r = drive_service.files().create(body=file_metadata, media_body=media, fields='id').execute()
                    #     except HttpError as err:
                    #         logging.info(str(chat_id)+err)
                    #         print(str(chat_id)+err)
                    #         if err.resp.status in [403, 500, 503]:
                    #             sleep(5)
                    #             try:
                    #                 r = drive_service.files().create(body=file_metadata, media_body=media, fields='id').execute()
                    #             except HttpError as err:
                    #                 logging.info(str(chat_id)+err+' повторно, пропускаем '+img)
                    #                 print(str(chat_id)+err+' повторно, пропускаем '+img)
                    #                 continue

                    # открываем доступ к папке с датой
                    # for email in emails:
                    #     batch = drive_service.new_batch_http_request(callback=callback)
                    #     user_permission = {
                    #         'type': 'user',
                    #         'role': 'reader',
                    #         'emailAddress': ''+email+''

                    #     }
                    #     batch.add(drive_service.permissions().create(
                    #             fileId=folder_id_last_date,
                    #             body=user_permission,
                    #             fields='id',
                    #     ))
                    #     batch.execute()

        # print(list_name_items)
        logging.info(list_name_items)
        logging.info(date_user)

        if not str(date_user) in str(list_name_items):
            # print('Нет '+ str(date_user))
            logging.info(str(chat_id) + ' ' + 'Нет ' + str(date_user))
            print(str(chat_id) + ' ' + 'Нет ' + str(date_user))
            # Создаем папки с датой в корневой папке
            file_metadata = {
                'name': date_user,
                'mimeType': 'application/vnd.google-apps.folder',
                'parents': [parentID]
            }
            file = drive_service.files().create(body=file_metadata, fields='id').execute()
            folder_name_user_id = file.get('id')
            # print ('Folder ID: %s' % file.get('id'))

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
            print(url)

            # копируем файлы в папку конечную
            logging.info(str(chat_id) + ' 285 Запускаю функцию загрузки фото на drive')
            img_upload_drive(chat_id, folder_name_date_user_id, pics, drive_service)
            logging.info(str(chat_id) + ' 285 Закончил')
            # for img in pics:
            #     name = img.split('/')[-1]
            #     #print(name)
            #     logging.info(str(chat_id)+'Копируем файл '+str(name))
            #     file_path = img
            #     file_metadata = {
            #                     'name': name,
            #                     'parents': [folder_name_date_user_id]
            #                 }
            #     media = MediaFileUpload(file_path, resumable=True)
            #     r = drive_service.files().create(body=file_metadata, media_body=media, fields='id').execute()

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
                response = request.execute()
                logging.info(str(chat_id) + 'Создан лист ' + str(date_user))
            except Exception as e:
                print(e)
                logging.info(e)
            # Заполняем заголовки
            try:

                values_title_sh = [['Дата фотоотчёта'], ['Город'], ['Модель'], ['Гос. номер'], ['Номер СТС'], ['ФИО'],
                                   ['Ссылка на актуальную папку']]
                results = sheets_service.spreadsheets().values().batchUpdate(spreadsheetId=SAMPLE_SPREADSHEET_ID, body={
                    "valueInputOption": "USER_ENTERED",
                    "data": [
                        {"range": "" + date_user + "!A1:J1",
                         "majorDimension": "COLUMNS",
                         # сначала заполнять ряды, затем столбцы (т.е. самые внутренние списки в values - это ряды)
                         "values": values_title_sh}
                    ]
                }).execute()

                # заполняем новый лист базовыми значения из базы данны(Город, Модель, Гос. номер, ФИО)
                sql = "SELECT * FROM car"
                cursor.execute(sql)
                line_sql = cursor.fetchall()
                count_id = len(line_sql)
                print(line_sql)

                logging.info(str(chat_id) + 'Кол-во записей в базе данных ' + str(count_id))
                print(str(chat_id) + 'Кол-во записей в базе данных ' + str(count_id))
                j = 2
                for line in line_sql:
                    results = sheets_service.spreadsheets().values().batchUpdate(spreadsheetId=SAMPLE_SPREADSHEET_ID,
                                                                                 body={
                                                                                     "valueInputOption": "USER_ENTERED",
                                                                                     "data": [
                                                                                         {
                                                                                             "range": "" + date_user + "!A" + str(
                                                                                                 j) + ":G" + str(j),
                                                                                             "majorDimension": "COLUMNS",
                                                                                             "values": [[line[6]],
                                                                                                        [line[1]],
                                                                                                        [line[2]],
                                                                                                        [line[3]],
                                                                                                        [line[9]],
                                                                                                        [line[4]],
                                                                                                        [line[8]]]}
                                                                                     ]
                                                                                 }).execute()
                    sleep(1)
                    # print(line)
                    # print(type(line[4]))
                    if line[4] is None:
                        logging.info(line[1] + ' ' + line[2] + ' ' + line[3])
                        print(line[1] + ' ' + line[2] + ' ' + line[3])
                        j += 1
                    else:
                        logging.info(line[1] + ' ' + line[2] + ' ' + line[3] + ' ' + line[4])
                        print(line[1] + ' ' + line[2] + ' ' + line[3] + ' ' + line[4])
                        j += 1
                logging.info(
                    str(chat_id) + 'Заполняем заголовок листа ' + str(date_user) + ' и основые данные из базы данных')

                spreadsheet = sheets_service.spreadsheets().get(spreadsheetId=SAMPLE_SPREADSHEET_ID).execute()
                sheetList = spreadsheet.get('sheets')
                for sheet in sheetList:
                    if sheet['properties']['title'] == date_user:
                        sheetId = sheet['properties']['sheetId']
                        # logging.info()
                        break

                results = sheets_service.spreadsheets().batchUpdate(spreadsheetId=SAMPLE_SPREADSHEET_ID,
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
                print(e)
                logging.info(e)

        # Проверяем что в текущей книге есть лист с нужной датой
        # results = sheets_service.spreadsheets().get(spreadsheetId = SAMPLE_SPREADSHEET_ID, fields='sheets.properties').execute()
        # print(results['sheets'])
        # logging.info(results['sheets'])
        # for sh in results['sheets']:
        #     sh=(sh['properties'])
        #     sh_name=sh['title']
        #     sh_id=sh['sheetId']

        #     if date_user == sh_name:
        #         print(sh_id)
        #         logging.info(sh_id)

        # заносим в sql ссылку на папку гугл диска
        conn = sqlite3.connect(PATH + "bot_sql.db")  # или :memory: чтобы сохранить в RAM
        cursor = conn.cursor()
        sql = """
        UPDATE car 
        SET last_url = \"""" + str(url) + """\"
        WHERE chat_id = '""" + str(id_user) + """'
            """
        cursor.execute(sql)
        conn.commit()

        spreadsheet = sheets_service.spreadsheets().get(spreadsheetId=SAMPLE_SPREADSHEET_ID).execute()
        sheetList = spreadsheet.get('sheets')
        for sheet in sheetList:
            if sheet['properties']['title'] == date_user:
                sheetId = sheet['properties']['sheetId']
                # logging.info()
                break

        try:
            range_ = date_user + '!D1:200'
            request = sheets_service.spreadsheets().values().get(spreadsheetId=SAMPLE_SPREADSHEET_ID, range=range_)
            response = request.execute()
            sleep(1)
            k = len(response["values"])
            r = response["values"]
            in_table = False
            for i in range(1, k):
                try:
                    if r[i][0] == number_car:
                        logging.info(
                            str(chat_id[0]) + ' Нашел номер ' + str(number_car) + ' в табилце ' + str(date_user))
                        print(str(chat_id[0]) + ' Нашел номер ' + str(number_car) + ' в табилце ' + str(date_user))
                        results = sheets_service.spreadsheets().batchUpdate(spreadsheetId=SAMPLE_SPREADSHEET_ID,
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
                        logging.info(str(chat_id) + ' Цвет строки ' + str(i + 1) + ' закрашен')
                        results = sheets_service.spreadsheets().values().batchUpdate(
                            spreadsheetId=SAMPLE_SPREADSHEET_ID, body={
                                "valueInputOption": "USER_ENTERED",
                                "data": [
                                    {"range": "" + date_user + "!A" + str(i + 1) + ":J" + str(i + 1),
                                     "majorDimension": "COLUMNS",
                                     # сначала заполнять ряды, затем столбцы (т.е. самые внутренние списки в values - это ряды)
                                     "values": [[alll[0][6]], [alll[0][1]], [alll[0][2]], [alll[0][3]], [alll[0][9]],
                                                [alll[0][4]], [url]]}
                                ]
                            }).execute()
                        logging.info(str(chat_id) + ' инф в строку ' + str(i + 1) + ' занесена ')
                        logging.info(str(chat_id) + ' Обновлены данные в листе ' + str(date_user))
                        in_table = True
                        break
                    # else:
                    #     print('Номера в таблице нет')
                except KeyError:
                    print('Пустая ячейка ' + range_)
                    logging.info('Пустая ячейка ' + range_)
                    break
            if in_table is False:
                results = sheets_service.spreadsheets().batchUpdate(spreadsheetId=SAMPLE_SPREADSHEET_ID,
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
                                                                                                    "startRowIndex": k,
                                                                                                    "endRowIndex": k + 1,
                                                                                                    "startColumnIndex": 0,
                                                                                                    "endColumnIndex": 7
                                                                                                },
                                                                                            "fields": "userEnteredFormat"
                                                                                        }
                                                                                }
                                                                            ]
                                                                    }).execute()
                results = sheets_service.spreadsheets().values().batchUpdate(spreadsheetId=SAMPLE_SPREADSHEET_ID, body={
                    "valueInputOption": "USER_ENTERED",
                    "data": [
                        {"range": "" + date_user + "!A" + str(k + 1) + ":J" + str(k + 1),
                         "majorDimension": "COLUMNS",
                         # сначала заполнять ряды, затем столбцы (т.е. самые внутренние списки в values - это ряды)
                         "values": [[alll[0][6]], [alll[0][1]], [alll[0][2]], [alll[0][3]], [alll[0][9]], [alll[0][4]],
                                    [url]]}
                    ]
                }).execute()
                logging.info(str(chat_id) + ' Записаны новые данные в лист ' + str(date_user))
        except Exception as e:
            print(e)


def callback(request_id, response, exception):
    if exception:
        print(exception)
        logging.info(exception)
    else:
        logging.info("Permission Id: %s" % response.get('id'))
