import json
import re
from datetime import datetime, timedelta

from motor.motor_asyncio import AsyncIOMotorClient
from telethon import TelegramClient
from telethon.events import NewMessage

app_id = 2017358
api_hash = 'e934477c749a3358801547eea8fcd3ce'
MONGO_URI = "mongodb://localhost:27017"

client = TelegramClient('bot_' + api_hash, app_id, api_hash)
mongo_client = AsyncIOMotorClient(MONGO_URI)

re_date = re.compile(r'\d{2}\.\d{2}\.\d{4}', re.DOTALL)

match = {'час': 'hour', 'ден': 'day', 'недел': 'week', 'месяц': 'month', 'год': 'year'}


async def aggregate_date(_from: datetime, _to: datetime, name: str) -> dict:
    dataset = []
    labels = []
    _id = {}
    sort = {'min': 1}
    for t in reversed(match.values()):
        if name in _id:
            break
        _id[t] = {"$" + ('dayOfWeek' if t == 'day' else t): "$dt"}
        # sort[f"_id.{t}"] = 1

    async for item in mongo_client.TEST.sample_collection.aggregate([
        {
            "$match": {
                "dt": {
                    "$gte": _from,
                    "$lte": _to
                }
            }
        },
        {
            "$group": {
                "_id": _id,
                "totalValue": {"$sum": "$value"},
                "min": {"$min": "$dt"}
            }
        },
        {
            "$sort": sort
        }
    ]):
        dt: datetime = item['min']
        c = {'microsecond': 0, 'second': 0, 'minute': 0, 'hour': 0, 'day': 1, 'month': 0, 'year': 0}
        for t in reversed(_id):
            if t in c:
                del c[t]
        dataset.append(item['totalValue'])
        labels.append(dt.replace(**c).strftime('%Y-%m-%dT%H:%M:%S'))
    return {
        "dataset": dataset,
        "labels": labels
    }


@client.on(NewMessage())
async def new_message(event: NewMessage.Event):
    print(event.chat_id)
    print(event.message.text)
    if not event.message.text:
        await client.send_message(event.chat_id, 'Укажите даты выборки')
        return

    try:
        data = json.loads(event.message.text)

        if not isinstance(data, dict):
            await client.send_message(event.chat_id, 'Некорректный словарь')
            return

        if not ("dt_from" in data and "dt_upto" in data and "group_type" in data):
            await client.send_message(event.chat_id, 'Некорректный запрос')
            return

        group_type = data['group_type']

        if group_type not in match.values():
            await client.send_message(event.chat_id, 'Неизвестный тип агрегации')
            return

        try:
            first_date = datetime.strptime(data["dt_from"], '%Y-%m-%dT%H:%M:%S')
            second_date = datetime.strptime(data["dt_upto"], '%Y-%m-%dT%H:%M:%S')
        except ValueError:
            await client.send_message(event.chat_id, 'Дата должна быть указана в формате ISO')
            return

    except json.decoder.JSONDecodeError:
        results = re_date.findall(event.message.text)
        if len(results) != 2:
            await client.send_message(event.chat_id, 'Укажите даты выборки')
            return

        first_date = datetime.strptime(results[0], '%d.%m.%Y')
        second_date = datetime.strptime(results[1], '%d.%m.%Y')

        if first_date > second_date:
            second_date, first_date = first_date, second_date

        second_date += timedelta(hours=23, minutes=59, seconds=59, microseconds=99)

        results = re.findall(r'(час|ден|недел|месяц)', event.message.text, re.DOTALL | re.IGNORECASE)

        agg_names = []
        for result in results:
            v = result.lower()
            if v in match:
                agg_names.append(match[v])

        if not agg_names:
            await client.send_message(event.chat_id, 'Укажите тип агрегирования: час, день, неделю, месяц')
            return

        group_type = agg_names[-1]

    data = await aggregate_date(first_date, second_date, group_type)
    print(data)
    if data:
        await client.send_message(event.chat_id, json.dumps(data))
    else:
        await client.send_message(event.chat_id, 'Ничего не найдено')

# https://t.me/TestBox123123_bot
client.start(bot_token='7496866754:AAEC0GRKo7QOMdFip-755mn65IaPaAiRKVc')
client.run_until_disconnected()
