from time import time
import os.path

import pandas as pd
from QuikPy import QuikPy  # Работа с QUIK из Python через LUA скрипты QuikSharp


# noinspection PyShadowingNames
def get_candles_from_provider(qp_provider, class_code, security_code, tf) -> pd.DataFrame:
    """Получение бар из провайдера

    :param QuikPy qp_provider: Провайдер QUIK
    :param str class_code: Код режима торгов
    :param str security_code: Код тикера
    :param str tf: Временной интервал https://ru.wikipedia.org/wiki/Таймфрейм
    """
    time_frame, _ = qp_provider.timeframe_to_quik_timeframe(tf)  # Временной интервал QUIK
    # logger.info(f'Получение истории {class_code}.{security_code} {tf} из QUIK')
    history = qp_provider.get_candles_from_data_source(class_code, security_code, time_frame)  # Получаем все бары из QUIK
    if not history:  # Если бары не получены
        # logger.error('Ошибка при получении истории: История не получена')
        return pd.DataFrame()  # то выходим, дальше не продолжаем
    if 'data' not in history:  # Если бар нет в словаре
        # logger.error(f'Ошибка при получении истории: {history}')
        return pd.DataFrame()  # то выходим, дальше не продолжаем
    new_bars = history['data']  # Получаем все бары из QUIK
    if len(new_bars) == 0:  # Если новых бар нет
        # logger.info('Новых записей нет')
        return pd.DataFrame()  # то выходим, дальше не продолжаем
    pd_bars = pd.json_normalize(new_bars)  # Переводим список бар в pandas DataFrame
    pd_bars.rename(columns={'datetime.year': 'year', 'datetime.month': 'month', 'datetime.day': 'day',
                            'datetime.hour': 'hour', 'datetime.min': 'minute', 'datetime.sec': 'second'},
                   inplace=True)  # Чтобы получить дату/время переименовываем колонки
    pd_bars['datetime'] = pd.to_datetime(pd_bars[['year', 'month', 'day', 'hour', 'minute', 'second']])  # Собираем дату/время из колонок
    pd_bars = pd_bars[['datetime', 'open', 'high', 'low', 'close', 'volume']]  # Отбираем нужные колонки. Дата и время нужны, чтобы не удалять одинаковые OHLCV на разное время
    pd_bars.index = pd_bars['datetime']  # Дата/время также будет индексом
    pd_bars.volume = pd.to_numeric(pd_bars.volume, downcast='integer')  # Объемы могут быть только целыми
    pd_bars.drop_duplicates(keep='last', inplace=True)  # Могут быть получены дубли, удаляем их
    # logger.info(f'Первый бар    : {pd_bars.index[0]:{dt_format}}')
    # logger.info(f'Последний бар : {pd_bars.index[-1]:{dt_format}}')
    # logger.info(f'Кол-во бар    : {len(pd_bars)}')
    return pd_bars


def get_bar(class_code='TQBR', security_code='SBER', timeFrame='D', compression=1,
                      skipFirstDate=False, skipLastDate=False, fourPriceDoji=False):
    """Получение баров

    :param classCode: Код рынка
    :param secCodes: Коды тикеров в виде кортежа
    :param timeFrame: Временной интервал 'M'-Минуты, 'D'-дни, 'W'-недели, 'MN'-месяцы
    :param compression: Кол-во минут для минутного графика. Для остальных = 1
    :param skipFirstDate: Убрать бары на первую полученную дату
    :param skipLastDate: Убрать бары на последнюю полученную дату
    :param fourPriceDoji: Оставить бары с дожи 4-х цен
    """
    # Получаем бары из провайдера
    pd_bars = get_candles_from_provider(qp_provider, class_code, security_code, tf='M5')  
    print(pd_bars)

if __name__ == '__main__':  # Точка входа при запуске этого скрипта
    start_time = time()  # Время начала запуска скрипта

    interval = 5

    # Вызываем конструктор QuikPy с подключением к локальному компьютеру с QUIK
    qp_provider = QuikPy()  
    # Вызываем конструктор QuikPy с подключением к удаленному компьютеру с QUIK
    # qpProvider = QuikPy(Host='<Ваш IP адрес>')  

    # classCode = 'TQBR'  # Акции ММВБ
    class_code = 'SPBFUT'  # Фьючерсы РТС
    # secCodes = ('GAZP',)  # Для тестов

    # TOP 40 акций ММВБ
    # secCodes = ('GAZP', 'LKOH', 'SBER', 'NVTK', 'YNDX', 'GMKN', 'ROSN', 'MTLR', 'MGNT', 'CHMF',
    #             'PHOR', 'VTBR', 'TCSG', 'PLZL', 'ALRS', 'MAGN', 'CBOM', 'SMLT', 'MVID', 'AFLT',
    #             'SNGS', 'SBERP', 'NLMK', 'RUAL', 'MTSS', 'TATN', 'MOEX', 'VKCO', 'MTLRP', 'AFKS',
    #             'SNGSP', 'PIKK', 'ISKJ', 'OZON', 'POLY', 'HYDR', 'RASP', 'IRAO', 'SIBN', 'FESH')  
    # Формат фьючерса: <Тикер><Месяц экспирации><Последняя цифра года> 
    # Месяц экспирации: 3-H, 6-M, 9-U, 12-Z
    security_code = 'RIH5'  

    # Получаем бары в первый раз / когда идет сессия
    # Дневные бары
    # SaveCandlesToFile(classCode, secCodes, skipLastDate=True, fourPriceDoji=True) 
    # 15-и минутные бары
    # SaveCandlesToFile(classCode, secCodes, 'M', 15, skipFirstDate=True, skipLastDate=True) 
    # 5-и минутные бары 
    get_bar(class_code, security_code, 'M', interval, skipFirstDate=True, skipLastDate=True)  

    # Получаем бары, когда сессия не идет
    # SaveCandlesToFile(classCode, secCodes, fourPriceDoji=True)  # Дневные бары
    # SaveCandlesToFile(classCode, secCodes, 'M', 15, skipFirstDate=True)  # 15-и минутные бары
    # SaveCandlesToFile(classCode, secCodes, 'M', 5, skipFirstDate=True)  # 5-и минутные бары

    # Перед выходом закрываем соединение и поток QuikPy из любого экземпляра
    # Закрываем соединение для запросов и поток обработки функций обратного вызова
    qp_provider.close_connection_and_thread()  
    
    print(f'Скрипт выполнен за {(time() - start_time):.2f} с')
