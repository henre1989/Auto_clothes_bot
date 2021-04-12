import telebot
from telebot import types
import sqlite3
from datetime import datetime
import os
import logging
from logging import handlers
from time import sleep
from d_s import main
import threading
import locale
from telethon import TelegramClient
import asyncio

PATH = os.getcwd() + "/settings/"
f = open(PATH + 'settings.txt', encoding='utf-8')
f_line = f.readlines()
f.close()

logging.getLogger("urllib3").setLevel(logging.ERROR)
logging.basicConfig(handlers=[handlers.RotatingFileHandler(filename=PATH + "bot.log", encoding='utf-8', mode='a+',
                                                           maxBytes=10 * 1024 * 1024)],
                    format="%(asctime)s %(name)s:%(levelname)s:%(message)s",
                    level=logging.INFO)

API_TOKEN = (f_line[0].split('=')[1]).strip('\n')
CATEGORIES = ['Фотоотчёт авто', 'Видеоотчёт по одежде']
bot = telebot.TeleBot(API_TOKEN)
user_dict = {}
chat_id_agent = (f_line[5].split('=')[1]).strip('\n')
id_bot = (f_line[6].split('=')[1]).strip('\n')
api_id = (f_line[7].split('=')[1]).strip('\n')
api_hash = (f_line[8].split('=')[1]).strip('\n')


class Queue:
    def __init__(self):
        self.items = []

    def isEmpty(self):
        return self.items == []

    def enqueue(self, item):
        self.items.insert(0, item)

    def dequeue(self):
        return self.items.pop()

    def size(self):
        return len(self.items)

q = Queue()



class User:
    def __init__(self, name):
        self.name = name
        self.category = None
        self.model = None
        self.car_number = None
        self.fio = None
        self.city = None
        self.date = None
        self.num_STS = None
        self.pic = []


async def main_agent(date):
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    client = TelegramClient('settings/bot.session', int(api_id), str(api_hash), loop=loop)
    await client.start()
    chat_id = int(id_bot)
    messages =await client.get_messages(chat_id)
    chat_id_user = messages[0].fwd_from.from_id.user_id
    type_content = messages[0].media.document.mime_type.split('/')[-1]
    path = 'settings/' +str(chat_id_user)+'/photo_clothes/'+date+'/1.'+type_content
    await client.download_media(messages[0], file=path)
    await client.disconnect()
    return path


def date_for_chothes():
    try:
        t = open(PATH + 'folder_clothes.txt', 'r', encoding='utf-8')
        return t.read()
    except Exception as e:
        logging.info('date_for_chothes ' + str(e))


def one_massage():
    while True:
        f = open(PATH + 'day_clothes.txt', encoding='utf-8')
        day_message = f.read()
        f.close()
        today = datetime.now().date()
        logging.info('Проверка даты для оповещения по одежде ' + str(day_message))
        logging.info('Сегодня ' + str(today) + ' ' + str(datetime.now().hour) + ' часов')
        if day_message == str(today):
            if today.hour == 13:
                year = today.isocalendar()[0]
                week = today.isocalendar()[1] + 8
                day = today.isocalendar()[-1]
                if day in [6, 7]:
                    week += 1
                end_day = str((datetime.strptime("%d%d%d" % (year, week, 1), "%Y%W%w")).date())
                logging.info('Дата следующей проверки одежды '+ str(end_day))
                f = open(PATH + 'day_clothes.txt', 'w', encoding='utf-8')
                f.write(end_day)
                f.close()
                t = open(PATH + 'folder_clothes.txt', 'w', encoding='utf-8')
                t.write(end_day)
                t.close()
                logging.info('Файл day_clothes.txt перезаписан')
                logging.info('Файл folder_clothes.txt перезаписан')
                sql = "SELECT * FROM clothes"
                alll = sql_requests(sql)
                for str_chat_id in alll:
                    chat_id = str(str_chat_id[0])
                    if chat_id == 'None':
                        continue
                    elif chat_id == '':
                        continue
                    try:
                        locale.setlocale(locale.LC_ALL,'ru_RU.UTF-8')
                        today = datetime.today().strftime("%d %b")
                        bot.send_message(chat_id, 'Для того, что бы понимать в каком состоянии у вас корпоративная одежда и '
                                                  'одели ли вы ее в принципе, 5-6 раз в год будет приходить сообщение в Telegram с '
                                                  'произвольным кодовым словом. Вам, в течение часа, необходимо снять видео и '
                                                  'произнести данный код. \nКодовое слово на сегодня: '+str(today))
                        logging.info(str(chat_id) + ' оповещен')
                        sleep(1)
                    except Exception as e:
                        logging.info(e)
                        logging.info(str(chat_id) + ' заблокировал бота или нет такого пользователя')
        sleep(3600)


def global_num_sts(message):
    number_STS = message.text
    alf = 'абвгдеёжзийклмнопрстуфхцчшщъыьэюя1234567890'
    s1 = number_STS[2:4].lower()
    if message.text is None:
        pass
    elif len(message.text) != 10:
        pass
    elif s1[0] not in alf:
        pass
    elif s1[1] not in alf:
        pass
    else:
        return number_STS.upper()


def check_send_messages():
    sql = "SELECT * FROM employees"
    alll = sql_requests(sql)
    for str_chat_id in alll:
        chat_id = str(str_chat_id[0])
        if chat_id == 'None':
            continue
        elif chat_id == '':
            continue
        try:
            bot.send_message(chat_id, 'Бот перезапущен, для дальнейшей работы выполните команду /start')
            logging.info(str(chat_id) + ' оповещен')
            sleep(1)
        except Exception as e:
            logging.info(e)
            logging.info(str(chat_id) + ' заблокировал бота или нет такого пользователя')
    while True:
        sql = "SELECT * FROM car"
        alll = sql_requests(sql)
        for str_chat_id in alll:
            chat_id = str(str_chat_id[0])
            if chat_id == 'None':
                continue
            today = datetime.now()
            hour = today.hour
            day = today.isocalendar()[-1]
            if day == 1:
                if int(hour) == 11:
                    try:
                        bot.send_message(chat_id,
                                         'Здравствуйте. Напоминание о фотоотчёте корпоративного автомобиля Випсилинг.')
                        logging.info(str(chat_id) + ' оповещен о Напоминание')
                    except Exception as e:
                        logging.info(e)
                        logging.info(str(chat_id) + ' заблокировал бота или нет такого пользователя')
        sleep(3600)


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


@bot.message_handler(commands=['help', 'start'])
def send_welcome(message):
    try:
        chat_id = message.chat.id
        logging.info('Зашел ' + str(chat_id))
        sql = 'SELECT * FROM employees WHERE chat_id="' + str(chat_id) + '"'
        alll = sql_requests(sql)
        logging.info('Кол-во записей ' + str(chat_id) + ' в базе employees ' + str(len(alll)))
        keyboard = types.ReplyKeyboardMarkup(resize_keyboard=True, one_time_keyboard=True)
        if len(alll) > 0:
            keyboard.add(*[types.KeyboardButton(name) for name in CATEGORIES])
            bot.send_message(chat_id, 'Добро пожаловать ' + alll[0][1])
            msg = bot.reply_to(message, """\
                            Выберите раздел
                            """, reply_markup=keyboard)
            bot.register_next_step_handler(msg, check_car_and_clothes)
        else:
            keyboard.add(types.KeyboardButton('Регистрация сотрудника'))
            msg = bot.send_message(chat_id, 'Добро пожаловать ', reply_markup=keyboard)
            bot.register_next_step_handler(msg, reg_new_employees)
    except Exception as e:
        logging.info(str(chat_id) + ' send_welcome ' + str(e))


def reg_new_employees(message):
    try:
        chat_id = message.chat.id
        name = message.text
        user = User(name)
        user_dict[chat_id] = user
        logging.info(str(chat_id) + ' ' + str(name))
        if name is None:
            msg = bot.reply_to(message, 'Введены неверные данные, попробуйте еще раз')
            bot.register_next_step_handler(msg, reg_new_employees)
        else:
            logging.info(str(chat_id) + ' ' + str(message.text))
            msg = bot.send_message(chat_id, 'Введите свое ФИО')
            bot.register_next_step_handler(msg, reg_new_employees_fio)
    except Exception as e:
        logging.info(str(chat_id) + ' reg_new_employees ' + str(e))
        msg = bot.reply_to(message, 'Введены неверные данные, попробуйте еще раз')
        bot.register_next_step_handler(msg, reg_new_employees)


def reg_new_employees_fio(message):
    try:
        chat_id = message.chat.id
        fio = message.text
        logging.info(str(chat_id) + ' ' + str(fio))
        if fio is None:
            msg = bot.reply_to(message, 'Введены неверные данные, попробуйте еще раз')
            bot.register_next_step_handler(msg, reg_new_employees_fio)
        else:
            logging.info(str(chat_id) + ' ' + str(message.text))
            alf = 'абвгдеёжзийклмнопрстуфхцчшщъыьэюя'
            text = fio.lower()
            name_spl = text.split(' ')
            if len(name_spl) == 3:
                for spl1 in name_spl[0]:
                    if not spl1 in alf:
                        msg = bot.reply_to(message, 'в поле ФИО введены недопустимые символы, ввод только кириллицей')
                        bot.register_next_step_handler(msg, reg_new_employees_fio)
                        return
                    for spl2 in name_spl[1]:
                        if not spl2 in alf:
                            msg = bot.reply_to(message,
                                               'в поле ФИО введены недопустимые символы, ввод только кириллицей')
                            bot.register_next_step_handler(msg, reg_new_employees_fio)
                            return
                        for spl3 in name_spl[2]:
                            if not spl3 in alf:
                                msg = bot.reply_to(message,
                                                   'в поле ФИО введены недопустимые символы, ввод только кириллицей')
                                bot.register_next_step_handler(msg, reg_new_employees_fio)
                                return
                Name = ''
                for i in name_spl:
                    name_i = i.capitalize()
                    Name = Name + name_i + ' '
                user_dict[chat_id].fio = Name
                logging.info(str(chat_id) + ' ' + str(message.text))
                msg = bot.send_message(chat_id, 'Укажите город, в котором вы работаете')
                bot.register_next_step_handler(msg, reg_new_employees_city)
            else:
                msg = bot.reply_to(message, 'Введены неверные данные, попробуйте еще раз')
                bot.register_next_step_handler(msg, reg_new_employees_fio)
    except Exception as e:
        logging.info(str(chat_id) + ' reg_new_employees_fio ' + str(e))
        msg = bot.reply_to(message, 'Введены неверные данные, попробуйте еще раз')
        bot.register_next_step_handler(msg, reg_new_employees_fio)


def reg_new_employees_city(message):
    try:
        chat_id = message.chat.id
        city = message.text
        user_dict[chat_id].city = city
        user = user_dict[chat_id]
        logging.info(str(chat_id) + ' ' + str(city))
        if city is None:
            msg = bot.reply_to(message, 'Введены неверные данные, попробуйте еще раз')
            bot.register_next_step_handler(msg, reg_new_employees_city)
        else:
            logging.info(str(chat_id) + ' ' + str(message.text))
            sql = """INSERT INTO employees VALUES ('""" + str(
                chat_id) + """', '""" + user.fio + """' , '""" + user.city + """')"""
            sql_requests(sql)
            keyboard = types.ReplyKeyboardMarkup(resize_keyboard=True, one_time_keyboard=True)
            keyboard.add(types.KeyboardButton('Завершить регистрацию'))
            msg = bot.send_message(chat_id, 'Нажмите кнопку "Завершить регистрацию"', reply_markup=keyboard)
            bot.register_next_step_handler(msg, send_welcome)
    except Exception as e:
        logging.info(str(chat_id) + ' reg_new_employees_city ' + str(e))
        msg = bot.reply_to(message, 'Введены неверные данные, попробуйте еще раз')
        bot.register_next_step_handler(msg, reg_new_employees_city)


def check_data_about_car(message):
    keyboard = types.ReplyKeyboardMarkup(resize_keyboard=True, one_time_keyboard=True)
    try:
        chat_id = message.chat.id
        name = message.text
        logging.info(str(chat_id) + ' ' + str(name))
        if name == 'Нет':
            keyboard.add(
                *[types.KeyboardButton(name) for name in ['Ravon R2', 'Daewoo Matiz', 'Lada Largus', 'Renault Cangoo']])
            logging.info(str(chat_id) + ' ' + str(message.text))
            msg = bot.send_message(chat_id, 'Выберите модель машины, если вашей модели в списке нет, напишите вручную',
                                   reply_markup=keyboard)
            bot.register_next_step_handler(msg, add_model_car)
        else:
            keyboard.add(*[types.KeyboardButton(name) for name in ['/Зaгрузить']])
            photo = open(PATH + 'basic_photo.jpg', 'rb')
            today = datetime.now()
            week = today.isocalendar()[1] - 1
            day = today.isocalendar()[-1]
            if day in [6, 7]:
                week += 1
            bot.send_photo(chat_id, photo)
            photo.close()
            bot.reply_to(message, 'Прикрепите 3 новых фото автомобиля согласно образцу и нажмите Зaгрузить',
                         reply_markup=keyboard)
    except Exception as e:
        logging.info(str(chat_id) + ' check_data_about_car ' + str(e))
        msg = bot.reply_to(message, 'Введены неверные данные, попробуйте еще раз')
        bot.register_next_step_handler(msg, check_data_about_car)


def check_car_number(chat_id, message):
    car_number = message.text
    if message.text is None:
        pass
    else:
        logging.info(str(chat_id) + ' ' + str(message.text))
        alf = ['А', 'В', 'Е', 'К', 'М', 'Н', 'О', 'Р', 'С', 'Т', 'У', 'Х']
        s1 = car_number[:1].upper()
        s2 = car_number[1:4]
        s3 = car_number[4:6].upper()
        if len(car_number) == 8:
            s4 = car_number[6:8]
        elif len(car_number) == 9:
            s4 = car_number[6:9]
        else:
            s4 = 'A'
        if s1 in alf:
            if s2.isdigit():
                if s3[0] in alf:
                    if s3[1] in alf:
                        if s4.isdigit():
                            return car_number.upper()
                        else:
                            pass
                    else:
                        pass
                else:
                    pass
            else:
                pass
        else:
            pass


def delete_car_from_base(chat_id, car_number):
    try:
        sql = "SELECT * FROM car WHERE car_number='" + str(car_number) + "'"
        logging.info(str(chat_id) + ' ' + sql)
        alll = sql_requests(sql)
        cars = len(alll)
        car_in_base = alll
        if cars > 0:
            # удаляем из базы строку
            logging.info(str(chat_id) + ' Нашел ' + str(cars) + ' записи с номером ' + car_number)
            for i in range(0, cars):
                sql = "DELETE FROM car WHERE car_number='" + str(car_number) + "'"
                sql_requests(sql)
                logging.info(str(chat_id) + ' удаляет ' + str(car_in_base[i]))
    except Exception as e:
        logging.info(str(chat_id) + ' Записей с номером ' + str(car_number) + ' не найдено')


@bot.message_handler(commands=['Зaгрузить'])
def upload_pic_to_drive(message):
    try:
        chat_id = message.chat.id
        user = user_dict[chat_id]
        today = datetime.now()
        data = today.date()
        if user.name == CATEGORIES[1]:
            data_now = date_for_chothes()
            bot.send_message(chat_id, 'Видео загружается... подождите несколько секунд')
            table = 'clothes'
            if len(user.pic) == 0:
                msg = bot.reply_to(message, 'Прикрепите видео и нажмите Загрузить')
                bot.register_next_step_handler(msg, send_media)
                return
        elif user.name == CATEGORIES[0]:
            year = today.isocalendar()[0]
            week = today.isocalendar()[1] - 1
            day = today.isocalendar()[-1]
            if day in [6, 7]:
                week += 1
            data_now = str((datetime.strptime("%d%d%d" % (year, week, 1), "%Y%W%w")).date())
            bot.send_message(chat_id, 'Фото загружается... подождите несколько секунд')
            table = 'car'
            if len(user.pic) == 0:
                msg = bot.reply_to(message, 'Прикрепите фото и нажмите Загрузить')
                bot.register_next_step_handler(msg, send_media)
                return

        logging.info(str(chat_id) + ' ' + str(user.pic))
        sql = """
        UPDATE """ + table + """ 
        SET list_pic = \"""" + str(user.pic) + """\"
        WHERE chat_id = '""" + str(chat_id) + """'
            """
        sql_d = """
            UPDATE """ + table + """ 
            SET data = '""" + str(data) + """'
            WHERE chat_id = '""" + str(chat_id) + """'
            """
        sql_d_n = """
            UPDATE """ + table + """ 
            SET data_now = '""" + str(data_now) + """'
            WHERE chat_id = '""" + str(chat_id) + """'
            """
        sql_requests(sql)
        logging.info('sql ок')
        sql_requests(sql_d)
        logging.info('sql_d ок')
        sql_requests(sql_d_n)
        logging.info('sql_d_n ок')
        main(chat_id, user.name)
        keyboard = types.ReplyKeyboardMarkup(resize_keyboard=True, one_time_keyboard=False)
        keyboard.add(*[types.KeyboardButton(name) for name in ['В главное меню']])
        msg = bot.send_message(chat_id, 'Данные обновлены', reply_markup=keyboard)
        logging.info(str(chat_id) + ' ' + str(user.pic) + ' Данные обновлены')
        bot.register_next_step_handler(msg, send_welcome)
    except Exception as e:
        logging.info(str(chat_id) + ' upload_pic_to_drive ' + str(e))
        bot.send_message(chat_id, 'Ошибка, выполните команду /start')


def check_car_and_clothes(message):
    keyboard = types.ReplyKeyboardMarkup(resize_keyboard=True, one_time_keyboard=True)
    try:
        chat_id = message.chat.id
        name = message.text
        if name == CATEGORIES[1]:
            user = User(name)
            user_dict[chat_id] = user
            user_dict[chat_id].category = name
            keyboard.add(*[types.KeyboardButton(name) for name in ['/Зaгрузить']])
            bot.send_message(chat_id, 'Необходимо снять видео и произнести кодовое слово и отправить (дождитесь '
                                      'сообщение от бота об окончании обработки видео). '
                                      'На нем мы должны четко '
                                      'видеть поло/флис/ветровка или зимнюю куртку, в зависимости от погоды!',
                             reply_markup=keyboard)
        elif name == CATEGORIES[0]:
            user = User(name)
            user_dict[chat_id] = user
            user_dict[chat_id].category = name
            keyboard.add(
                *[types.KeyboardButton(name) for name in ['Ravon R2', 'Daewoo Matiz', 'Lada Largus', 'Renault Cangoo']])
            # записать категорию в класс
            sql = 'SELECT * FROM car WHERE chat_id="' + str(chat_id) + '"'
            cars = sql_requests(sql)
            if len(cars) > 0:
                keyboard = types.ReplyKeyboardMarkup(resize_keyboard=True, one_time_keyboard=True)
                keyboard.add(*[types.KeyboardButton(name) for name in ['Да', 'Нет']])
                logging.info(str(chat_id) + ' ' + str(message.text))
                alll = cars
                model = alll[0][1]
                number = alll[0][2]
                number_STS = alll[0][7]
                bot.send_message(chat_id, (
                    'Модель авто: {}\n Гос номер авто: {}\n Номер СТС: {}').format(
                    model,
                    number,
                    number_STS
                ))
                msg = bot.reply_to(message, 'Подтвердите что эти данные актуальны для вас', reply_markup=keyboard)
                bot.register_next_step_handler(msg, check_data_about_car)
            else:
                bot.reply_to(message, 'Данных об автомобиле не обнаружено')
                msg = bot.send_message(chat_id,
                                       'Выберите модель машины, если вашей модели в списке нет, напишите вручную',
                                       reply_markup=keyboard)
                bot.register_next_step_handler(msg, add_model_car)
    except Exception as e:
        logging.info(str(chat_id) + ' check_car_and_clothes ' + str(e))
        msg = bot.reply_to(message, 'Введены неверные данные, попробуйте еще раз')
        bot.register_next_step_handler(msg, check_car_and_clothes)


def add_model_car(message):
    try:
        chat_id = message.chat.id
        model = message.text
        if message.text is None:
            msg = bot.reply_to(message, 'Введены неверные данные, попробуйте еще раз')
            bot.register_next_step_handler(msg, add_model_car)
        else:
            logging.info(str(chat_id) + ' ' + str(message.text))
            user = user_dict[chat_id]
            user.model = model
            msg = bot.reply_to(message, 'Гос.номер автомобиля (например, А012РН196)',
                               reply_markup=types.ReplyKeyboardRemove())
            bot.register_next_step_handler(msg, add_car_number)
    except Exception as e:
        logging.info(str(chat_id) + ' add_model_car ' + str(e))
        msg = bot.reply_to(message, 'Введены неверные данные, попробуйте еще раз')
        bot.register_next_step_handler(msg, add_model_car)


def add_car_number(message):
    try:
        chat_id = message.chat.id
        user = user_dict[chat_id]
        user.car_number = check_car_number(chat_id, message)
        if user.car_number is None:
            msg = bot.reply_to(message,
                               'Номер не соответствует российскому формату номеров, попробуйте ввести номер еще раз')
            bot.register_next_step_handler(msg, add_car_number)
        else:
            msg = bot.reply_to(message, 'Введите номер СТС (например 00ХХ000000 или 0000000000)',
                               reply_markup=types.ReplyKeyboardRemove())
            bot.register_next_step_handler(msg, add_num_sts)
    except Exception as e:
        logging.info(str(chat_id) + ' add_car_number ' + str(e))
        msg = bot.reply_to(message, 'Введены неверные данные, попробуйте еще раз')
        bot.register_next_step_handler(msg, add_car_number)


def delete_duplicate_in_base(chat_id):
    # читаем старые данные из базы по этому id
    logging.info(str(chat_id) + ' Удаляем дубликаты')
    user = user_dict[chat_id]
    sql = 'SELECT * FROM car WHERE chat_id="' + str(chat_id) + '"'
    old_car = sql_requests(sql)
    if len(old_car) > 0:
        logging.info(str(chat_id) + ' Старые данные ' + str(old_car[0]))
        delete_car_from_base(chat_id, old_car[0][2])
        # создаем строчку с моделью номером городом стс и датами фото старой
        sql = ("""INSERT INTO car
                          VALUES ('""""""', '""" + old_car[0][1] + """', '""" + old_car[0][2] +
               """', '""" + old_car[0][3] + """', '""" + old_car[0][4] + """',  \"""" + old_car[0][
                   5] + """\", '""" + old_car[0][6] + """', '""" + old_car[0][7] + """')""")
        sql_requests(sql)
        logging.info(str(chat_id) + ' Создали запись старых данных ' + old_car[0][2] + ' в базе car')
        # Проверка наличия строк с данным id
        sql = "SELECT * FROM car WHERE car_number='" + str(user.car_number) + "'"
        alll = sql_requests(sql)
        logging.info(str(chat_id) + ' Кол-во записей ' + str(user.car_number) + ' в базе  car ' + str(len(alll)))
        sql = """SELECT * FROM car WHERE car_number='""" + str(user.car_number) + """' AND chat_id = '""" + str(
            chat_id) + """'"""
        kol_act = sql_requests(sql)

        if len(alll) == 0:
            sql = ("""INSERT INTO car
                          VALUES ('""" + str(chat_id) + """', '""" + str(
                user.model) + """', '""" + str(user.car_number) +
                   """', '""" + old_car[0][3] + """', '""" + old_car[0][4] + """', \"""" + old_car[0][5] +
                   """\", '""" + old_car[0][6] + """', '""" + str(user.num_STS) + """')""")
            sql_requests(sql)
            logging.info(str(chat_id) + ' не блыло записей в базе car, запись создана ' + str(user.car_number))
        # проверяем на наличие актуального id и номера машины, создаем если записей нет
        elif len(kol_act) == 0:
            delete_car_from_base(chat_id, user.car_number)
            sql = ("""INSERT INTO car
                          VALUES ('""" + str(chat_id) + """', '""" + str(
                user.model) + """', '""" + str(user.car_number) +
                   """', '""" + old_car[0][3] + """', '""" + old_car[0][4] + """', \"""" + old_car[0][5] +
                   """\", '""" + old_car[0][6] + """', '""" + str(user.num_STS) + """')""")
            sql_requests(sql)
            logging.info(str(chat_id) + ' не блыло записей в базе car, запись создана ' + str(user.car_number))
    else:
        logging.info(str(chat_id) + ' Записей не найдено в талице car')


def add_data_in_car_resp(car_number, city):
    try:
        logging.info(car_number + ' ' + city)
        sql = 'SELECT city FROM car_responsible WHERE car_number="' + car_number + '"'
        logging.info(sql_requests(sql))
        count_cars = sql_requests(sql)[0]
        if len(count_cars) > 0:
            sql = """ UPDATE car_responsible SET city = """ + city + """ WHERE car_number = '""" + car_number + """' """
            logging.info(sql)
            sql_requests(sql)
            logging.info('обновил данные в базе car_responsible')
        else:
            sql = """INSERT INTO car_responsible VALUES ('""" + car_number + """', '""" + city + """')"""
            sql_requests(sql)
            logging.info(sql)
            logging.info('добавил данные в базе car_responsible')
    except Exception as e:
        logging.info('add_data_in_car_resp ' + str(e))


def add_num_sts(message):
    try:
        chat_id = message.chat.id
        user = user_dict[chat_id]
        user.num_STS = global_num_sts(message)
        delete_duplicate_in_base(chat_id)
        if user.num_STS is None:
            msg = bot.reply_to(message, 'Введены неверные данные, попробуйте еще раз')
            bot.register_next_step_handler(msg, add_num_sts)
        else:

            logging.info(str(chat_id) + ' ' + str(message.text))
            user = user_dict[chat_id]
            today = datetime.now()
            data_now = today.date()
            year = today.isocalendar()[0]
            week = today.isocalendar()[1] - 1
            day = today.isocalendar()[-1]
            if day in [6, 7]:
                week += 1
            user.date = str((datetime.strptime("%d%d%d" % (year, week, 1), "%Y%W%w")).date())
            delete_car_from_base(chat_id, user.car_number)

            sql = ("""INSERT INTO car VALUES ('""" + str(
                chat_id) + """', '""" + user.model + """', '""" + user.car_number + """', '""" + user.date + """',
                 '""" + str(data_now) + """', '""""""', '""""""', '""" + user.num_STS + """')""")
            sql_requests(sql)
            logging.info(str(chat_id) + ' добовление в базу car успешно')
            sql = 'SELECT city FROM employees WHERE chat_id="' + str(chat_id) + '"'
            logging.info(sql)
            city = sql_requests(sql)[0][0]
            logging.info(city)
            add_data_in_car_resp(user.car_number, city)
            bot.send_message(chat_id, (
                'Модель авто: {}\n Гос номер авто: {}\n Номер СТС: {}\n Дата: {}\n ').format(
                user.model,
                user.car_number,
                user.num_STS,
                data_now))
            logging.info(str(chat_id) + ' ' + user.name + ' ' + str(
                user.model) + ' ' + user.car_number + ' ' + user.num_STS + ' ' + str(data_now) + ' Записано')
            keyboard = types.ReplyKeyboardMarkup(resize_keyboard=True, one_time_keyboard=True)
            keyboard.add(types.KeyboardButton('Завершить'))
            msg = bot.send_message(chat_id, 'Нажмите кнопку "Завершить"', reply_markup=keyboard)
            bot.register_next_step_handler(msg, send_welcome)
    except Exception as e:
        logging.info(str(chat_id) + ' add_num_sts ' + str(e))
        msg = bot.reply_to(message, 'Введены неверные данные, попробуйте еще раз')
        bot.register_next_step_handler(msg, add_num_sts)


@bot.message_handler(content_types=['photo', 'video'])
def Queues(message):
    q.enqueue(message)


def working_queeue():
    while True:
        while not q.isEmpty():
            logging.info('длина очереди для записи обработки медиа ' + str(q.size()))
            send_media()
        sleep(1)


def send_media():
    try:
        message = q.dequeue()
        path = PATH
        chat_id = message.chat.id
        try:
            logging.info(str(chat_id) + ' тип контента ' + message.content_type)
        except Exception:
            pass
        try:
            mess = message.text
            user = user_dict[chat_id]
        except KeyError:
            user = User(mess)
            user_dict[chat_id] = user
        Path = path + str(chat_id)
        today = datetime.now()
        year = today.isocalendar()[0]
        week = today.isocalendar()[1] - 1
        day = today.isocalendar()[-1]
        if day in [6, 7]:
            week += 1
        date_now = str((datetime.strptime("%d%d%d" % (year, week, 1), "%Y%W%w")).date())
        try:
            os.mkdir(Path)
        except IOError:
            pass
        if user.category == CATEGORIES[0]:
            logging.info(str(chat_id) + ' категория ' + user.category)
            if message.content_type == 'photo':
                logging.info(str(chat_id) + ' тип контента ' + message.content_type)
                try:
                    os.mkdir(Path + '/photo_auto')
                except IOError:
                    pass
                try:
                    os.mkdir(Path + '/photo_auto/' + date_now)
                except IOError:
                    pass
                photo = message.photo[-1]
                file_info = bot.get_file(photo.file_id)
                file_name = file_info.file_path.strip('photos')
                src = Path + '/photo_auto/' + date_now + file_name
                downloaded_file = bot.download_file(file_info.file_path)
                with open(src, 'wb') as new_file:
                    new_file.write(downloaded_file)
            else:
                msg = bot.reply_to(message, 'Не верный тип контента, загрузите фотографии и нажимите кнопку '
                                            'Загрузить')
                bot.register_next_step_handler(msg, send_media)
        elif user.category == CATEGORIES[1]:
            logging.info(str(chat_id) + ' категория ' + user.category)
            try:
                date_now = date_for_chothes()
            except Exception:
                t = open(PATH + 'folder_clothes.txt', 'w', encoding='utf-8')
                t.write(str(today.date()))
                t.close()
                date_now = date_for_chothes()
            if message.content_type == 'video':
                logging.info(str(chat_id) + ' тип контента ' + message.content_type)
                try:
                    os.mkdir(Path + '/photo_clothes')
                except IOError:
                    pass
                try:
                    os.mkdir(Path + '/photo_clothes/' + date_now)
                except IOError:
                    pass
                video = message.video
                message_id = message.id
                #до 20 мб
                size_video = int(video.file_size)/1024/1024
                if size_video > 20:
                    msg = bot.forward_message(chat_id_agent, chat_id, message_id)
                    src = asyncio.run(main_agent(date_now))
                    logging.info('Путь к файлу ' + src)
                    bot.delete_message(chat_id_agent, msg.message_id)
                    bot.send_message(chat_id, ' Видео обработано, нажмите кнопку Загрузить')
                else:
                    logging.info(str(chat_id) + 'Размер видео ' + str(size_video))
                    file_info = bot.get_file(video.file_id)
                    file_name = file_info.file_path.strip('videos')
                    src = Path + '/photo_clothes' + '/' + date_now + file_name
                    downloaded_file = bot.download_file(file_info.file_path)
                    with open(src, 'wb') as new_file:
                        new_file.write(downloaded_file)
                    bot.send_message(chat_id, ' Видео обработано, нажмите кнопку Загрузить')
                sql = 'SELECT * FROM clothes WHERE chat_id="' + str(chat_id) + '"'
                cl_list = sql_requests(sql)
                if len(cl_list) > 0:
                    sql = """UPDATE clothes
                                                    SET data = '""" + str(date_now) + """', data_now = '""" + str(
                        today.date()) + """', list_pic = \"""" + src + """\"
                                                    WHERE chat_id = '""" + str(chat_id) + """'
                                                    """
                    sql_requests(sql)
                    logging.info(str(chat_id) + ' в базе данных в теблице clothes, данные обновелны')
                else:
                    sql = """INSERT INTO clothes VALUES ('""" + str(
                        chat_id) + """', '""" + str(date_now) + """' , '""" + str(today.date()) + """',
                        '""" + src + """', '')"""
                    sql_requests(sql)
                    logging.info(str(chat_id) + ' в базе данных в теблице clothes, занесена новая инфорамция')
            else:
                msg = bot.reply_to(message, 'Не верный тип контента, загрузите видео и нажимите кнопку Загрузить')
                bot.register_next_step_handler(msg, send_media)
        user.pic.append(src)
        logging.info("файл добавлен " + src)
    except Exception as e:
        logging.info(str(chat_id) + ' send_media ' + str(e))
        return


def start_bot():
    while True:
        try:
            bot.polling(none_stop=True, timeout=300)
        except ConnectionError as e:
            logging.info(e)
            sleep(15)
        except Exception as e:
            logging.info(e)
            sleep(15)


if __name__ == '__main__':
    t1 = threading.Thread(target=start_bot)
    t2 = threading.Thread(target=check_send_messages)
    t3 = threading.Thread(target=one_massage)
    t4 = threading.Thread(target=working_queeue)
    t1.start()
    t2.start()
    t3.start()
    t4.start()
