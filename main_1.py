from datetime import datetime  # Дата и время
from time import time

import pandas as pd
from QuikPy import QuikPy  # Работа с QUIK из Python через LUA скрипты QuikSharp


def changed_connection(data):
    """Пользовательский обработчик событий:
    - Соединение установлено
    - Соединение разорвано
    """
    print(f'{datetime.now().strftime("%d.%m.%Y %H:%M:%S")} - {data}')  # Печатаем полученные данные


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
        self.df_bars = self.df_bars.iloc[:-1]  # Удаление последней строки
        print(self.df_bars)
        # print(self.df_bars['datetime'].dtype)
        # return self.df_bars

    def new_bar_callback(self, data):
        """Пользовательский обработчик событий:
        - Изменение стакана котировок
        - Получение обезличенной сделки
        - Получение новой свечки
        """
        if data['data']['interval'] == 5:
            # Преобразуем дату и время
            datetime_str = datetime(
                year=data['data']['datetime']['year'],
                month=data['data']['datetime']['month'],
                day=data['data']['datetime']['day'],
                hour=data['data']['datetime']['hour'],
                minute=data['data']['datetime']['min'],
                second=data['data']['datetime']['sec']
            )

            # Создаем датафрейм
            df = pd.DataFrame([{
                'datetime': datetime_str,
                'open': data['data']['open'],
                'high': data['data']['high'],
                'low': data['data']['low'],
                'close': data['data']['close'],
                'volume': data['data']['volume']
            }])

            df.index = df['datetime']  # Дата/время также будет индексом
            print(df)
            # print(df['datetime'].dtype)
            self.df_bars = (
                pd.concat([self.df_bars, df])
                .drop_duplicates(subset=['datetime'])
                .sort_index(ascending=True)
                )
            print(self.df_bars)


if __name__ == '__main__':  # Точка входа при запуске этого скрипта
    start_time = time()  # Время начала запуска скрипта

    # interval = 5

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
    # print(gmts.df_bars)

    # Просмотр изменений состояния соединения терминала QUIK с сервером брокера
    qp_provider.on_connected = changed_connection  # Нажимаем кнопку "Установить соединение" в QUIK
    qp_provider.on_disconnected = changed_connection  # Нажимаем кн. "Разорвать соединение" в QUIK

    # Подписка на новые свечки. При первой подписке получим все свечки с начала прошлой сессии
    qp_provider.on_new_candle = gmts.new_bar_callback  # Обработчик получения новой свечки

    for interval in (1, 5,):  # (1, 60, 1440) = Минутки, часовки, дневки
        print(f'Подписка на интервал {interval}: '
              f'{qp_provider.subscribe_to_candles(class_code, security_code, interval)["data"]}')
        print(f'Статус подписки на интервал {interval}: '
              f'{qp_provider.is_subscribed(class_code, security_code, interval)["data"]}')

    input('Enter - отмена\n')
    for interval in (1, 5,):  # (1, 60, 1440) = Минутки, часовки, дневки
        print(f'Отмена подписки на интервал {interval} '
              f'{qp_provider.unsubscribe_from_candles(class_code, security_code, interval)["data"]}')
        print(f'Статус подписки на интервал {interval}: '
              f'{qp_provider.is_subscribed(class_code, security_code, interval)["data"]}')

    # Перед выходом закрываем соединение и поток QuikPy из любого экземпляра
    # Закрываем соединение для запросов и поток обработки функций обратного вызова
    qp_provider.close_connection_and_thread()  

    print(f'Скрипт выполнен за {(time() - start_time):.2f} с')
