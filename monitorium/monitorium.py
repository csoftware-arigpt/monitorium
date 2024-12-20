from pyrogram import Client, filters
import logging
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine, Column, Integer, String, MetaData, Table, Date, BigInteger, Float, Boolean, Text
from sqlalchemy import inspect
import os
import random
import json
import telebot
from telebot import types
import asyncio
from pyrogram import errors
import threading
import time
import traceback
from sqlalchemy.exc import SQLAlchemyError
import datetime
import itertools

logger = logging.getLogger(__name__)

def create_channels_base():
    try:
        logger.info('Get info from environments')

        username = os.environ.get('MYSQL_USER')
        password = os.environ.get('MYSQL_PASS')
        host = os.environ.get('MYSQL_HOST')
        database = os.environ.get('MYSQL_DATABASE')
        logger.info('Connecting to database')
        engine = create_engine(f"mysql+pymysql://{username}:{password}@{host}/{database}")
        inspector = inspect(engine)
        Session = sessionmaker(bind=engine)
        session = Session()
        metadata = MetaData()
        logger.info('Creating table')
        new_table = Table(
            "monitorBase", metadata,
            Column('id', BigInteger, primary_key=True),
            Column('creationDate', Date),
            Column('name', String(255)),
            Column('url', String(255)),
            Column('subscribers', BigInteger),
            Column('involvement', Float),
            Column('pings', BigInteger),
            Column('commentsPerMonth', Float),
            Column('postsPerMonth', Float),
            Column('avgReactions', Float),
            Column('isVerified', Boolean)
        )

        if not inspector.has_table("monitorBase"):
            logger.info('Committing changes')
            metadata.create_all(engine)
            session.commit()
            session.close()
        else:
            logger.info('Already exists. Skipping')
            session.close()
        return 'ok'
    except Exception as e:
        logger.error(e)
        exit()

def get_links_from_monitorbase():
    try:
        logger.info("Connecting to the database")
        username = os.environ.get('MYSQL_USER')
        password = os.environ.get('MYSQL_PASS')
        host = os.environ.get('MYSQL_HOST')
        database = os.environ.get('MYSQL_DATABASE')
        engine = create_engine(f"mysql+pymysql://{username}:{password}@{host}/{database}")
        metadata = MetaData()
        metadata.reflect(bind=engine)

        monitor_base_table = Table('monitorBase', metadata, autoload_with=engine)
        Session = sessionmaker(bind=engine)
        session = Session()
        logger.info("Fetching URLs from monitorBase table")
        links = session.query(monitor_base_table.c.url).all()
        session.close()

        return [link[0] for link in links]
    except Exception as e:
        logger.error(f"Error fetching URLs from monitorBase: {e}")
        return []

def create_monitor_database(chat_id):
    try:
        logger.info('Get info from environments')

        username = os.environ.get('MYSQL_USER')
        password = os.environ.get('MYSQL_PASS')
        host = os.environ.get('MYSQL_HOST')
        database = os.environ.get('MYSQL_DATABASE')
        engine = create_engine(f"mysql+pymysql://{username}:{password}@{host}/{database}")
        inspector = inspect(engine)
        Session = sessionmaker(bind=engine)
        session = Session()
        metadata = MetaData()
        logger.info('Creating table')
        new_table = Table(
            str(chat_id), metadata,
            Column('id', BigInteger, primary_key=True),
            Column('date', Date),
            Column('reactions', BigInteger),
            Column('comments', BigInteger),
            Column('views', BigInteger),
            Column('text', Text),
            Column('is_deleted', Boolean)
        )


        if not inspector.has_table(str(chat_id)):
            logger.info('Committing changes')
            metadata.create_all(engine)
            session.commit()
            session.close()
        else:
            logger.info('Already exists. Skipping')
            session.close()
        return 'ok'
    except Exception as e:
        logger.error(e)
        exit()

def check_if_exists(chat_id):
    try:
        logger.info('Get info from environments')
        username = os.environ.get('MYSQL_USER')
        password = os.environ.get('MYSQL_PASS')
        host = os.environ.get('MYSQL_HOST')
        database = os.environ.get('MYSQL_DATABASE')
        engine = create_engine(f"mysql+pymysql://{username}:{password}@{host}/{database}")
        inspector = inspect(engine)
        if inspector.has_table(str(chat_id)):
            return True
        else:
            return False
    except Exception as e:
        logger.error(e)
        exit()

def add_messages_to_db(chat_id, messages):
    try:
        username = os.environ.get('MYSQL_USER')
        password = os.environ.get('MYSQL_PASS')
        host = os.environ.get('MYSQL_HOST')
        database = os.environ.get('MYSQL_DATABASE')
        engine = create_engine(f"mysql+pymysql://{username}:{password}@{host}/{database}")
        metadata = MetaData()
        metadata.reflect(bind=engine)

        message_table = metadata.tables.get(str(chat_id))
        if not message_table:
            raise ValueError(f"Table for chat {chat_id} not found.")

        session = sessionmaker(bind=engine)()

        for msg in messages:
            session.execute(message_table.insert().values(
                id=msg.get("id"),
                date=msg.get("date"),
                views=msg.get("views", 0),
                reactions=msg.get("reactions", 0),
                comments=msg.get("comments", 0),
                text=msg.get("text", ""),
                is_deleted=msg.get("is_deleted", False)
            ))
        session.commit()
        session.close()
        logger.info(f"Messages added to table {chat_id}.")
    except Exception as e:
        logger.error(f"Error while adding messages to the database: {e}")

def update_monitorbase(chat_id, metrics):
    try:
        chat_id = int(str(chat_id).replace("-100", ""))
        username = os.environ.get('MYSQL_USER')
        password = os.environ.get('MYSQL_PASS')
        host = os.environ.get('MYSQL_HOST')
        database = os.environ.get('MYSQL_DATABASE')
        engine = create_engine(f"mysql+pymysql://{username}:{password}@{host}/{database}")
        metadata = MetaData()
        metadata.reflect(bind=engine)

        monitor_base_table = metadata.tables.get("monitorBase")

        session = sessionmaker(bind=engine)()
        update_stmt = (
            monitor_base_table.update()
            .where(monitor_base_table.c.id == chat_id)
            .values(metrics)
        )
        session.execute(update_stmt)
        session.commit()
        session.close()
        logger.info(f"monitorBase table updated for chat {chat_id}.")
    except Exception as e:
        logger.error(f"Error while updating monitorBase for chat {chat_id}: {e}")

async def get_chat_info(link):
    try:
        logger.info('Selecting session')
        sessions = [f for f in os.listdir("monitorium/sessions") if f.endswith('.session')]
        select_session = sessions[random.randint(0, len(sessions) - 1)]
        full_path = os.path.abspath(f"monitorium/sessions/{select_session}")
        json_path = full_path.replace(".session", ".json")

        with open(json_path, 'r') as file:
            json_data = json.load(file)

        proxy_file = os.environ.get('PROXY_FILE')
        proxy = None

        if proxy_file:
            logger.info('Running with proxy')
            with open(proxy_file, 'r') as file:
                json_proxy = json.load(file)
            selected_proxy = random.choice(json_proxy)
            proxy = {
                "scheme": selected_proxy['scheme'],
                "hostname": selected_proxy["host"],
                "port": selected_proxy["port"],
                "username": selected_proxy['login'],
                "password": selected_proxy['password'],
            }
        else:
            logger.info('Running without proxy')

        async with Client(
            full_path.replace(".session", ""),
            api_id=json_data['app_id'],
            api_hash=json_data['app_hash'],
            app_version=json_data['app_version'],
            device_model=json_data['device'],
            system_version=json_data['sdk'],
            lang_code=json_data['lang_code'],
            proxy=proxy
        ) as client:
            if ("+" in link) or ("joinchat/" in link):
                logger.info("Cannot use private chats")
                return "Приватные чаты недоступны"

            link = link.replace('t.me', "").replace('/', "").replace('.', '')
            chat = await client.get_chat(f"@{link}")

            if chat.type in ["bot", "private"]:
                logger.info("Attempt to add a bot or private account")
                return "Боты и пользователи не могут быть добавлены"

            first_message_info = await client.get_messages(chat.id, 1)
            chat_info = {
                "id": chat.id,
                "date": first_message_info.date.strftime("%d-%m-%y %H:%M:%S"),
                "name": chat.title or chat.username or "Unnamed",
                "subscribers": chat.members_count or 0,
                "isVerified": chat.is_verified or False,
                "link": f"@{link}"
            }
            logger.info("Chat info successfully fetched")
            return chat_info
    except Exception as e:
        logger.error(e)
        return None

def get_monitor_data(channel_id):
    try:
        channel_id = int(str(channel_id).replace("-100", ""))
        logger.info(f"Fetching data for channel ID: {channel_id}")
        username = os.environ.get('MYSQL_USER')
        password = os.environ.get('MYSQL_PASS')
        host = os.environ.get('MYSQL_HOST')
        database = os.environ.get('MYSQL_DATABASE')
        engine = create_engine(f"mysql+pymysql://{username}:{password}@{host}/{database}")
        metadata = MetaData()
        metadata.reflect(bind=engine)

        monitor_base_table = Table('monitorBase', metadata, autoload_with=engine)
        Session = sessionmaker(bind=engine)
        session = Session()

        result = session.query(monitor_base_table).filter(monitor_base_table.c.id == channel_id).first()
        session.close()

        if result:
            data = {column.name: getattr(result, column.name) for column in monitor_base_table.columns}
            return data
        else:
            logger.info(f"No data found for channel ID: {channel_id}")
            return None
    except Exception as e:
        logger.error(f"Error fetching data for channel ID {channel_id}: {e}")
        return None

def save_and_calculate(chat, messages):
    try:
        logger.info(f"Processing data for chat {chat.id}")

        add_messages_to_db(chat.id, messages)

        now = datetime.datetime.now()
        last_30_days = [msg for msg in messages if msg["date"] >= now - datetime.timedelta(days=30)]
        total_messages = len(last_30_days)

        if total_messages == 0:
            logger.info(f"No messages in the last 30 days for chat {chat.id}.")
            return

        subscribers = chat.members_count or 0
        avg_views = sum(msg["views"] for msg in last_30_days) / total_messages
        engagement_rate = (avg_views / subscribers) * 100 if subscribers else 0
        posts_per_day = total_messages / 30
        avg_reactions = sum(msg["reactions"] for msg in last_30_days) / total_messages
        avg_comments = sum(msg["comments"] for msg in last_30_days) / total_messages

        metrics = {
            "involvement": engagement_rate,
            "commentsPerMonth": avg_comments,
            "postsPerMonth": posts_per_day,
            "avgReactions": avg_reactions,
        }
        update_monitorbase(chat.id, metrics)

        logger.info(f"Metrics successfully saved for chat {chat.id}: {metrics}")
    except Exception as e:
        logger.error(f"Error while processing chat {chat.id}: {e}")

async def monitoring_session():
    try:
        logger.info('Selecting session')
        sessions = [f for f in os.listdir("monitorium/sessions") if f.endswith('.session')]
        selected_session = sessions[random.randint(0, len(sessions) - 1)]
        full_path = os.path.abspath(f"monitorium/sessions/{selected_session}")
        json_path = full_path.replace(".session", ".json")
        
        with open(json_path, 'r') as file:
            json_data = json.load(file)
        
        proxy_file = os.environ.get('PROXY_FILE')
        proxy = None

        if proxy_file:
            logger.info('Running with proxy')
            with open(proxy_file, 'r') as file:
                json_proxy = json.load(file)
            selected_proxy = random.choice(json_proxy)
            proxy = {
                "scheme": selected_proxy['scheme'],
                "hostname": selected_proxy["host"],
                "port": selected_proxy["port"],
                "username": selected_proxy.get('login'),
                "password": selected_proxy.get('password'),
            }

        async with Client(
            full_path.replace(".session", ""),
            api_id=json_data['app_id'],
            api_hash=json_data['app_hash'],
            app_version=json_data['app_version'],
            device_model=json_data['device'],
            system_version=json_data['sdk'],
            lang_code=json_data['lang_code'],
            proxy=proxy
        ) as client:
            links = get_links_from_monitorbase()
            for link in links:
                try:
                    chat = await client.get_chat(link)
                    
                    logger.info(f"Monitoring channel: {chat.title} ({chat.id})")

                    messages = []
                    async for message in client.get_chat_history(chat.id):
                        reactions_count = 0
                        try:
                            reactions_list = message.reactions.reactions
                            reactions_count = sum(reaction.count for reaction in reactions_list) if reactions_list else 0
                        except Exception as e:
                            logger.error(e)
                        
                        messages.append({
                            "id": message.id,
                            "date": message.date,
                            "views": message.views or 0,
                            "reactions": reactions_count,
                            "comments": message.forwards or 0,
                            "text": message.text or "",
                            "is_deleted": False
                        })

                    save_and_calculate(chat, messages)

                except Exception as e:
                    tbe = traceback.TracebackException.from_exception(e)
                    stack_frames = traceback.extract_stack()
                    tbe.stack.extend(stack_frames)
                    formatted_traceback = ''.join(tbe.format())
                    logger.error(f"Error processing link: {formatted_traceback}")

    except Exception as e:
        logger.error(f"Error in monitoring_session: {e}")


def add_to_monitorbase(chat_info):
    try:
        logger.info(f"Adding channel {chat_info['id']} to monitorBase")
        username = os.environ.get('MYSQL_USER')
        password = os.environ.get('MYSQL_PASS')
        host = os.environ.get('MYSQL_HOST')
        database = os.environ.get('MYSQL_DATABASE')
        engine = create_engine(f"mysql+pymysql://{username}:{password}@{host}/{database}")
        metadata = MetaData()
        metadata.reflect(bind=engine)

        monitor_base_table = Table('monitorBase', metadata, autoload_with=engine)
        Session = sessionmaker(bind=engine)
        session = Session()

        new_entry = monitor_base_table.insert().values(
            id=int(str(chat_info['id']).replace("-100", "")),
            creationDate=chat_info.get('date', None),
            name=chat_info.get('name', 'Unnamed'),
            url=chat_info['link'],
            subscribers=chat_info.get('subscribers', 0),
            involvement=0.0,
            pings=0,
            commentsPerMonth=0.0,
            postsPerMonth=0.0,
            isVerified=chat_info.get('isVerified', False)
        )
        session.execute(new_entry)
        session.commit()
        session.close()
        logger.info(f"Channel {chat_info['id']} successfully added to monitorBase")
        return True
    except Exception as e:
        logger.error(f"Error adding channel {chat_info['id']} to monitorBase: {e}")
        return False

def telegramBot():
    try:
        logger.info("Running telegram bot")
        token = os.environ.get("TELEGRAM_TOKEN")
        bot = telebot.TeleBot(token)

        @bot.message_handler(commands=['start'])
        def start_command(message):
            bot.send_message(
                message.chat.id,
                "Добро пожаловать в Monitorium\nОтправьте ссылку на канал/чат для анализа."
            )

        @bot.message_handler(func=lambda message: message.text.lower() == 'me')
        def handle_me(message):
            bot.send_message(message.chat.id, "Неееее, так не работает")

        @bot.message_handler(func=lambda message: 'bot' in message.text.lower())
        def handle_bot(message):
            bot.send_message(message.chat.id, "Функция недоступна")

        @bot.message_handler(func=lambda message: True)
        def handle_links(message):
                if "t.me" not in message.text:
                    bot.send_message(
                        message.chat.id,
                        "Невалидная ссылка\nПример:\n\nt.me/chat\nchat.t.me"
                    )
                else:
                    bot.send_message(message.chat.id, "Ссылка получена, начинаем анализ...")
                    for i in range(0, 10):
                        chat_info = asyncio.run(get_chat_info(message.text))
                        if chat_info is None:
                            logger.error(f"Error / Trying again")
                            if i == 9:
                                bot.send_message(message.chat.id, "Произошла ошибка")
                        elif chat_info:
                            bot.send_message(message.chat.id, str(chat_info))
                            exists_in_database = check_if_exists(chat_info['id'])

                            if exists_in_database:
                                monitor_data = get_monitor_data(chat_info['id'])
                                if monitor_data:
                                    data_message = "\n".join([f"{key}: {value}" for key, value in monitor_data.items()])
                                    bot.send_message(message.chat.id, f"Данные из базы:\n{data_message}")
                                else:
                                    bot.send_message(message.chat.id, "Данные не найдены в базе")
                            else:
                                added = add_to_monitorbase(chat_info)
                                if added:
                                    bot.send_message(message.chat.id, f"Канал добавлен в список мониторинга: {chat_info['name']}")
                                    created = create_monitor_database(chat_info['id'])
                                    if created == 'ok':
                                        bot.send_message(message.chat.id, f"База данных для канала {chat_info['name']} создана успешно")
                                    else:
                                        bot.send_message(message.chat.id, f"Ошибка создания базы данных для канала {chat_info['name']}")
                                else:
                                    bot.send_message(message.chat.id, "Ошибка добавления канала в список мониторинга")
                        break
        while True:
                    try:
                        bot.polling(none_stop=True, timeout=90, interval=0)
                    except Exception as e:
                        logger.error(e)
                        time.sleep(5)
    except Exception as e:
        logger.error(e)
def brute_link():
   characters = 'abcdefghijklmnopqrstuvwxyz0123456789_'
   while True: 
    for length in range(4, 33):
         for combo in itertools.product(characters, repeat=length): 
            chat_info = asyncio.run(get_chat_info(f"{combo}.t.me"))
            for i in range(0, 10):
                if chat_info is None:
                                logger.error(f"Error / Trying again")
                elif chat_info['id']:
                    exists_in_database = check_if_exists(chat_info['id'])
                    if monitor_data:
                        logger.info('Found existing channel. Skipping')
                    else:
                        added = add_to_monitorbase(chat_info)
                        if added:
                            logger.info('Added channel to monitor -list')
                            created = create_monitor_database(chat_info['id'])
                            if created == "ok":
                                logger.info('Created monitor database')
                                time.sleep(5)

def run_monitor():
    asyncio.run(monitoring_session())

def main():
    os.remove("monitorium.log")
    logging.basicConfig(
        filename='monitorium.log',
        format="[{levelname}] - [{name}] - [{asctime}] - {message}",
        datefmt="%d-%m-%Y %H:%M:%S",
        style="{",
        level=logging.INFO
    )
    print('Monitorium - @csoftware - 2024')
    logger.info('Starting Monitorium')
    print("All logs in monitorium.log")
    create_channels_base()
    telegramThread = threading.Thread(target=telegramBot, args=())
    telegramThread.start()
    brutter = threading.Thread(target=brute_link, args=())
    brutter.start()
    while True:
        time_sleep = os.environ.get('SLEEP')
        monitorThread = threading.Thread(target=run_monitor, args=())
        monitorThread.start()
        monitorThread.join()
    
        time.sleep(int(time_sleep))
