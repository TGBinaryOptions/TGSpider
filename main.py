import asyncio
from datetime import datetime
from telethon import TelegramClient, events
import datetime
import re
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine, MetaData, Table, Integer, String, Column, DateTime, ForeignKey, Numeric,  SmallInteger
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship
from sqlalchemy.ext.automap import automap_base
from telethon.tl.types import Channel, Chat, User
from telethon import functions, types
from telethon.tl.types import ChannelParticipantsAdmins
import requests
import app_logger
import requests
from telethon.errors.rpcerrorlist import SessionPasswordNeededError
from database import DBManager, config


loop = asyncio.get_event_loop()
logger = app_logger.get_logger(__name__)
db_manager = DBManager()
db_manager.initialize()




async def get_messages(tg_client, entity, telegram_source, db_session):
    try:
        min_id = 0
        count = 0
        async for m in tg_client.iter_messages(entity, min_id=min_id, limit=None):
            msg = db_manager.Base.classes.messages()
            msg.telegram_sources_id = telegram_source.id
            msg.date = m.date
            msg.message = m.message
            msg.sender_id = m.sender_id
            msg.message_id = m.id
            msg.processing = False
            db_session.add(msg)
            count = count + 1
            if count % 10000 == 0:
                print(count)
        db_session.commit()
    except Exception as e:
        logger.info(e)



async def get_admins(tg_client, db_session, link, ts_id):
    # Many channels hide admins - so an error may appear here
    try:
        async for user in tg_client.iter_participants(link, filter=ChannelParticipantsAdmins):
            if not user.bot:
                # Add an admin to the list of admins
                ta = db_manager.Base.classes.telegram_admins()
                ta.telegram_id = user.id
                ta.nic = user.username
                if db_session.query(db_manager.Base.classes.telegram_admins).filter(
                        db_manager.Base.classes.telegram_admins.telegram_id == ta.telegram_id).first() is None:
                    db_session.add(ta)
                    db_session.commit()
                    db_session.flush()
                    # Adding a connection between the admin and the channel
                    sa = db_manager.Base.classes.sources_admins()
                    sa.sources_id = ts_id
                    sa.admins_id = ta.id
                    db_session.add(sa)
                    db_session.commit()
                else:
                    print('This admin is already in the database.')
                    # Adding a channel connection with an existing admin
                    sa = db_manager.Base.classes.sources_admins()
                    sa.sources_id = ts_id
                    sa.admins_id = db_session.query(db_manager.Base.classes.telegram_admins).filter(
                        db_manager.Base.classes.telegram_admins.telegram_id == ta.telegram_id).first().id
                    db_session.add(sa)
                    db_session.commit()
    except Exception as e:
        logger.info(e)

def add_to_telegram_tree(db_session, parent_id, child_id):
    if db_session.query(db_manager.Base.classes.telegram_tree).filter(
            db_manager.Base.classes.telegram_tree.parent_id == parent_id).filter(
        db_manager.Base.classes.telegram_tree.child_id == child_id).first() == None:
        print('Add to tree....')
        tt = db_manager.Base.classes.telegram_tree()
        tt.parent_id = parent_id
        tt.child_id = child_id
        db_session.add(tt)
        db_session.commit()



def check_tg(link):
    print(link)
    r = requests.get(link, verify=False)
    if 'View in Telegram' in r.text:
        return True
    else:
        return False


def post_processing(ts_id, db_session):
    logger.info(f"We process messages tg channel with id = {ts_id} in search of new links")
    ms = db_session.query(db_manager.Base.classes.messages).filter(
        db_manager.Base.classes.messages.processing == False).filter(
        db_manager.Base.classes.messages.telegram_sources_id == ts_id).all()
    ms_total_count = len(ms)
    logger.info(f"Total number of messages in the source: {ms_total_count}")
    links = []
    for m in ms:
        if m.message is not None:
            # Mark the message as processed
            db_session.query(db_manager.Base.classes.messages).filter(
                db_manager.Base.classes.messages.id == m.id).first().processing = True
            _nics = re.findall(r'@[a-zA-Z0-9_]{5,}|https://t.me\S+', m.message)
            for _nic in _nics:
                if '@' in _nic:
                    _nic = 'https://t.me/' + _nic.replace('@', '')
                    if _nic.lower() not in links:
                        links.append(_nic.lower())
    links_count = len(links)
    logger.info(f"Total number of potential links: {links_count}")
    potential_links = []
    for link in links:
        if check_tg(link):
            logger.info(link)
            potential_links.append(link)
            pts = db_manager.Base.classes.potential_telegram_sources()
            pts.link = link
            pts.parent_id = ts_id
            db_session.add(pts)
    check_links_count = len(potential_links)
    db_session.query(db_manager.Base.classes.telegram_sources).filter(
        db_manager.Base.classes.telegram_sources.id == ts_id).first().childs_count = check_links_count
    db_session.commit()
    logger.info(f"Total number of check links: {check_links_count}")



async def processing_telegram_source(link, parent_id, db_session, tg_client):
    # Check if there is a source with this link
    telegram_source = db_session.query(db_manager.Base.classes.telegram_sources).filter(
            db_manager.Base.classes.telegram_sources.link == link).first()
    if telegram_source is not None:
        logger.info('This channel is already in the list of sources')
        add_to_telegram_tree(db_session, parent_id, telegram_source.id)
    else:
        # It often happens that the telegram account no longer exists
        try:
            entity = await tg_client.get_entity(link)
            logger.info(f'processing {link} ...')
            #if entity.username is not None:
            username = entity.username
            logger.info(f'username:  {username}')
                # If the entity is a channel or group, then we take all messages and write them to the database
                # If entity is just a user then we do nothing
            if isinstance(entity, Channel):
                telegram_source = db_manager.Base.classes.telegram_sources()
                telegram_source.link = link
                telegram_source.username = username
                if entity.megagroup:
                    telegram_source.is_group = True
                    telegram_source.is_channel = False
                else:
                    telegram_source.is_group = False
                    telegram_source.is_channel = True
                telegram_source.telegram_id = entity.id
                telegram_source.caption = entity.title
                full_data = await tg_client(functions.channels.GetFullChannelRequest(entity))
                telegram_source.participants_count = full_data.full_chat.participants_count
                    # If this source is already in the database, then we do nothing
                ts_in_db = (db_session.query(db_manager.Base.classes.telegram_sources).filter
                                (db_manager.Base.classes.telegram_sources.username ==
                                 telegram_source.username).first())
                if ts_in_db is None:
                    telegram_source.date_processing = datetime.datetime.now()
                    db_session.add(telegram_source)
                    db_session.commit()
                    db_session.flush()
                        # Many channels hide admins - so an error may appear here
                    await get_admins(tg_client, db_session, link, telegram_source.id)
                        # Adding all messages from the channel to the database
                    await get_messages(tg_client, entity, telegram_source, db_session)
                    add_to_telegram_tree(db_session, parent_id, telegram_source.id)
                    post_processing(telegram_source.id, db_session)
                else:
                    logger.info('This channel is already in the database.')
                    add_to_telegram_tree(db_session, parent_id, ts_in_db.id)

            else:
                logger.info('This account is not a group, not a channel.')
        except Exception as e:
            logger.info(e)




async def main():
    logger.info("Start")
    tg_client = TelegramClient(config['TELEGRAM']['ACCOUNT_1']['session_name'],
                               config['TELEGRAM']['ACCOUNT_1']['api_id'], config['TELEGRAM']['ACCOUNT_1']['api_hash'])
    await tg_client.connect()
    if not await tg_client.is_user_authorized():
        await tg_client.send_code_request(config['TELEGRAM']['ACCOUNT_1']['phone_number'])
        try:
            await tg_client.sign_in(config['TELEGRAM']['ACCOUNT_1']['phone_number'], input('Enter the code: '))
        except SessionPasswordNeededError as err:
            await tg_client.sign_in(password=config['TELEGRAM']['ACCOUNT_1']['password'])
    db_session = db_manager.Session()
    ts = db_session.query(db_manager.Base.classes.potential_telegram_sources).filter(db_manager.Base.classes.potential_telegram_sources.is_processing == False).limit(100).all()
    for t in ts:
        print(t.link)
        #post_processing(5, db_session)
        await processing_telegram_source(t.link, t.parent_id, db_session, tg_client)
        t.is_processing = True
        db_session.commit()
    db_session.close()


loop.run_until_complete(main())



          
