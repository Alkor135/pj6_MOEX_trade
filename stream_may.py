from datetime import datetime  # Дата и время
import time  # Подписка на события по времени

from QuikPy import QuikPy  # Работа с QUIK из Python через LUA скрипты QUIK


def print_callback(data):
    """Пользовательский обработчик событий:
    - Изменение стакана котировок
    - Получение обезличенной сделки
    - Получение новой свечки
    """
    # print(f'{datetime.now().strftime("%d.%m.%Y %H:%M:%S")} - {data["data"]}')  # Печатаем полученные данные
    # Печатаем полученные данные
    # print(f'{datetime.now().strftime("%d.%m.%Y %H:%M:%S")} - {data["data"]['datetime']}')  
    if data['data']['interval'] == 5:
        print(f'{datetime.now().strftime("%d.%m.%Y %H:%M:%S")} - {data["data"]}')  # Печатаем полученные данные


def changed_connection(data):
    """Пользовательский обработчик событий:
    - Соединение установлено
    - Соединение разорвано
    """
    print(f'{datetime.now().strftime("%d.%m.%Y %H:%M:%S")} - {data}')  # Печатаем полученные данные


if __name__ == '__main__':  # Точка входа при запуске этого скрипта
    # Подключение к локальному запущенному терминалу QUIK по портам по умолчанию
    qp_provider = QuikPy()  

    class_code = 'SPBFUT'  # Класс тикера
    # Для фьючерсов: <Код тикера><Месяц экспирации: 3-H, 6-M, 9-U, 12-Z>
    # <Последняя цифра года>
    sec_code = 'RIH5'  

    # Просмотр изменений состояния соединения терминала QUIK с сервером брокера
    qp_provider.on_connected = changed_connection  # Нажимаем кнопку "Установить соединение" в QUIK
    qp_provider.on_disconnected = changed_connection  # Нажимаем кн. "Разорвать соединение" в QUIK

    # Подписка на новые свечки. При первой подписке получим все свечки с начала прошлой сессии
    qp_provider.on_new_candle = print_callback  # Обработчик получения новой свечки

    for interval in (1, 5,):  # (1, 60, 1440) = Минутки, часовки, дневки
        print(f'Подписка на интервал {interval}: '
              f'{qp_provider.subscribe_to_candles(class_code, sec_code, interval)["data"]}')
        print(f'Статус подписки на интервал {interval}: '
              f'{qp_provider.is_subscribed(class_code, sec_code, interval)["data"]}')

    input('Enter - отмена\n')
    for interval in (1, 5,):  # (1, 60, 1440) = Минутки, часовки, дневки
        print(f'Отмена подписки на интервал {interval} '
              f'{qp_provider.unsubscribe_from_candles(class_code, sec_code, interval)["data"]}')
        print(f'Статус подписки на интервал {interval}: '
              f'{qp_provider.is_subscribed(class_code, sec_code, interval)["data"]}')

    # Выход
    # Закрываем соединение для запросов и поток обработки функций обратного вызова
    qp_provider.close_connection_and_thread()
