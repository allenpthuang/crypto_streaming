from telethon import TelegramClient, events, sync
from datetime import datetime
import pandas as pd
import socket
import atexit
import json

# Have the same values on the other side! (Spark StreamingContext)
HOST = '127.0.0.1'
PORT = 55688

s =  socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.bind((HOST, PORT))
s.listen()
conn, addr = s.accept()
print(f'Connected by {addr}!\n')


def save_to_csv(messages_lst):
    filename = str(datetime.timestamp(datetime.now())) + '.csv'
    df = pd.DataFrame(messages_lst)
    df.to_csv(filename)


# Use your own values from my.telegram.org
api_id = <api_id>
api_hash = '<api_hash>'
client = TelegramClient('anon', api_id, api_hash)

group_name = ['OKX English',
              'Lucky Block (Official Telegram)',
              'Crypto Signals',
              'The Coin Farm',
              'BTC CHAMP',
              'Whalepool',
              'Fat Pig Signals',
              'ICO LISTING',
              'ICO SPEAKS']
messages_lst = []

# Upon exit, saves messages to a csv file, closes connection and socket
atexit.register(save_to_csv, messages_lst)
atexit.register(conn.close)
atexit.register(s.close)

@client.on(events.NewMessage(chats=group_name))
async def my_event_handler(event):
    print('[', event.message.date.isoformat(), ']', event.raw_text)
    messages_lst.append({'timestamp': event.message.date.isoformat(),
                         'group_name' : event.chat.title,
                         'message': event.raw_text})
    if messages_lst[-1]['message'] != '':
        to_send = json.dumps(messages_lst[-1]) + '\n'
        conn.sendall(to_send.encode('utf8'))

client.start()

for dialog in client.iter_dialogs():
    print(f'{dialog.id}:{dialog.title}')

client.run_until_disconnected()

# conn.close()
# s.close()
