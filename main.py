import asyncio
import json
import logging

from aiogram import Bot, Dispatcher, types, Router
from aiogram.filters import Command
from motor.motor_asyncio import AsyncIOMotorClient
from datetime import datetime

from pymongo.read_concern import ReadConcern
from pymongo.read_preferences import Primary

from utils import raise_date

bot_token = ""

router: Router = Router()

client = AsyncIOMotorClient('mongodb://localhost:27017/')
db = client['sampleDB']
collection = db['sample_collection']


async def aggregate(dt_from, dt_upto, group_type):
    """
    Функция принимает параметры для поиска по БД.
    :param dt_from: начальная дата.
    :param dt_upto: конечная дата.
    :param group_type: группировка.
    :return: список с зарплатами и датами.
    """

    if group_type not in ['month', 'day', 'hour']:
        raise ValueError("Ошибка. Группировка должна быть по 'hour', 'day', или 'month'.")

    pipeline = [
        {"$match": {"dt": {"$gte": datetime.fromisoformat(dt_from), "$lte": datetime.fromisoformat(dt_upto)}}},
        {"$group": {
            "_id": {"$dateTrunc": {"date": "$dt", "unit": group_type}},
            "totalValue": {"$sum": "$value"}
        }},
        {"$sort": {"_id": 1}}
    ]

    result: dict[str, list] = {
        'dataset': [],
        'labels': []
    }

    async with await client.start_session() as s:
        async with s.start_transaction(
            read_concern=ReadConcern(level='majority'),
            read_preference=Primary(),
        ):
            async for data in collection.aggregate(pipeline):
                amount: int = data.get('totalValue')
                dataset: list = result.get('dataset')
                labels: list = result.get('labels')
                date: str = datetime.isoformat(data.get('_id'))

                while datetime.fromisoformat(date) > datetime.fromisoformat(dt_from):
                    dataset.append(0)
                    labels.append(dt_from)
                    dt_from: str = await raise_date(group_type, dt_from)

                dataset.append(amount)
                labels.append(date)
                dt_from: str = await raise_date(group_type, dt_from)

            date: str = datetime.isoformat(data.get('_id'))
            date: str = await raise_date(group_type, date)

            if datetime.fromisoformat(date) <= datetime.fromisoformat(dt_upto):
                dataset.append(0)
                labels.append(date)
                date: str = await raise_date(group_type, date)

    return json.dumps(result)


@router.message(Command(commands=['start']))
async def send_welcome(message: types.Message) -> None:
    """
    Обработчик реагирует на /start и позволяет понять пользователю, что от него требуется.
    :param message: собщение начала работы бота.
    """
    await message.reply("Привет! Отправь мне данные в формате JSON для агрегации. Например:\n"
                        "{\"dt_from\": \"2022-10-01T00:00:00\", \"dt_upto\": \"2022-11-30T23:59:00\","
                        " \"group_type\": \"day\"}")

@router.message()
async def handle_input(message: types.Message):

    """
    Обработчик отлавливает и реагирует на любое сообщение от пользователя,
    если сообщение не соответствует типу JSON, отправляет об этом уведомление.
    :param message: любое сообщение от пользователя.
    """

    try:
        input_data = json.loads(message.text)
        result = await aggregate(input_data["dt_from"], input_data["dt_upto"], input_data["group_type"])
        await message.answer(f'{result}')
    except json.JSONDecodeError:
        await message.reply("Пожалуйста, отправь данные в правильном JSON формате.")
    except Exception as e:
        await message.reply(str(e))


async def main():
    bot: Bot = Bot(token=bot_token)

    dp: Dispatcher = Dispatcher()
    dp.include_router(router)

    try:
        await dp.start_polling(bot)
    finally:
        await bot.session.close()


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    asyncio.run(main())