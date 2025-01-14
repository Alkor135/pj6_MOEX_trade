from time import time

import pandas as pd
from QuikPy import QuikPy  # Работа с QUIK из Python через LUA скрипты QuikSharp


class Bars:
    def __init__(self, qp_provider, class_code, security_code, tf):
        self.qp_provider = qp_provider
        self.class_code = class_code
        self.sec_code = security_code
        self.tf = tf
        self.df_bars = pd.DataFrame()
        self.get_candles_from_provider()


    def get_candles_from_provider(self) -> pd.DataFrame:
        """
        Получение бар из провайдера
        """
        time_frame, _ = qp_provider.timeframe_to_quik_timeframe(self.tf)  # Временной интервал QUIK
        # Получаем все бары из QUIK
        history = qp_provider.get_candles_from_data_source(
            self.class_code, self.sec_code, time_frame
            )  
        if not history:  # Если бары не получены
            return pd.DataFrame()  # то выходим, дальше не продолжаем
        if 'data' not in history:  # Если бар нет в словаре
            return pd.DataFrame()  # то выходим, дальше не продолжаем
        new_bars = history['data']  # Получаем все бары из QUIK
        if len(new_bars) == 0:  # Если новых бар нет
            return pd.DataFrame()  # то выходим, дальше не продолжаем
        self.df_bars = pd.json_normalize(new_bars).tail(288)  # Переводим список бар в pandas DataFrame
        self.df_bars.rename(
            columns={'datetime.year': 'year', 'datetime.month': 'month', 'datetime.day': 'day',
            'datetime.hour': 'hour', 'datetime.min': 'minute', 'datetime.sec': 'second'}, inplace=True
            )  # Чтобы получить дату/время переименовываем колонки
        self.df_bars['datetime'] = pd.to_datetime(
            self.df_bars[['year', 'month', 'day', 'hour', 'minute', 'second']]
            )  # Собираем дату/время из колонок
        # Отбираем нужные колонки. Дата и время нужны, чтобы не удалять одинаковые OHLCV на разное время
        self.df_bars = self.df_bars[['datetime', 'open', 'high', 'low', 'close', 'volume']]  
        self.df_bars.index = self.df_bars['datetime']  # Дата/время также будет индексом
        self.df_bars.volume = pd.to_numeric(self.df_bars.volume, downcast='integer')  # Объемы могут быть только целыми
        self.df_bars.drop_duplicates(keep='last', inplace=True)  # Могут быть получены дубли, удаляем их
        # print(self.df_bars)
        # return self.df_bars


if __name__ == '__main__':  # Точка входа при запуске этого скрипта
    start_time = time()  # Время начала запуска скрипта

    interval = 5

    # Вызываем конструктор QuikPy с подключением к локальному компьютеру с QUIK
    qp_provider = QuikPy()  
    # Вызываем конструктор QuikPy с подключением к удаленному компьютеру с QUIK
    # qpProvider = QuikPy(Host='<Ваш IP адрес>')  

    class_code = 'SPBFUT'  # Фьючерсы РТС

    # Формат фьючерса: <Тикер><Месяц экспирации><Последняя цифра года> 
    # Месяц экспирации: 3-H, 6-M, 9-U, 12-Z
    security_code = 'RIH5'  

    # Получаем бары из истории QUIK
    # get_candles_from_provider(qp_provider, class_code, security_code, tf='M5')
    gmts = Bars(qp_provider, class_code, security_code, tf='M5')
    # gmts.get_candles_from_provider()
    print(gmts.df_bars)

    # Перед выходом закрываем соединение и поток QuikPy из любого экземпляра
    # Закрываем соединение для запросов и поток обработки функций обратного вызова
    qp_provider.close_connection_and_thread()  

    print(f'Скрипт выполнен за {(time() - start_time):.2f} с')
