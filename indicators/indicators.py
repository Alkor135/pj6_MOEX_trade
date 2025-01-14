import sqlite3

import pandas as pd
import numpy as np
import ta


# TVI -------------------------------------------------------------------------
def calculate_ema(series, period):
    """
    Вычисляет экспоненциальное скользящее среднее (EMA) 
    для указанного ряда данных.
    """
    return series.ewm(span=period, adjust=False).mean()

def add_tvi_column(df, r=12, s=12, u=5, point=0.0001):
    """
    Добавляет колонку TVI в DataFrame.
    
    :param df: DataFrame с колонками ['open', 'close', 'volume']
    :param r: Период для EMA для UpTicks и DownTicks
    :param s: Период для второго EMA
    :param u: Период для EMA на TVI_calculate
    :param point: Минимальное изменение цены (параметр MyPoint в MQL4)
    :return: DataFrame с добавленной колонкой 'TVI'
    """
    # Проверяем, что необходимые колонки существуют
    required_columns = ['open', 'close', 'volume']
    for col in required_columns:
        if col not in df.columns:
            raise ValueError(f"DataFrame должен содержать колонку '{col}'")

    # Вычисляем UpTicks и DownTicks
    df['UpTicks'] = (df['volume'] + (df['close'] - df['open']) / point) / 2
    df['DownTicks'] = df['volume'] - df['UpTicks']

    # EMA для UpTicks и DownTicks
    df['EMA_UpTicks'] = calculate_ema(df['UpTicks'], r)
    df['EMA_DownTicks'] = calculate_ema(df['DownTicks'], r)

    # Второе EMA для UpTicks и DownTicks
    df['DEMA_UpTicks'] = calculate_ema(df['EMA_UpTicks'], s)
    df['DEMA_DownTicks'] = calculate_ema(df['EMA_DownTicks'], s)

    # Расчет TVI_calculate
    df['TVI_calculate'] = 100.0 * (df['DEMA_UpTicks'] - df['DEMA_DownTicks']) / (
        df['DEMA_UpTicks'] + df['DEMA_DownTicks']
    )

    # EMA на TVI_calculate
    df['TVI'] = calculate_ema(df['TVI_calculate'], u)

    # Удаляем промежуточные колонки, если они не нужны
    df.drop(
        columns=['UpTicks', 'DownTicks', 'EMA_UpTicks', 'EMA_DownTicks', 
        'DEMA_UpTicks', 'DEMA_DownTicks', 'TVI_calculate'], inplace=True
        )

    return df

# GHL -------------------------------------------------------------------------
def calculate_sma(series, period):
    """
    Вычисляет простую скользящую среднюю (SMA).
    """
    return series.rolling(window=period, min_periods=1).mean()

def add_gann_hilo_column_optimized(df, period=10):
    """
    Оптимизированная версия добавления индикатора GannHiLo в DataFrame.
    
    :param df: DataFrame с колонками ['close', 'high', 'low']
    :param period: Период для расчета SMA
    :return: DataFrame с добавленными колонками 'Gann_HiLo'
    """
    # Проверка на необходимые колонки
    required_columns = ['close', 'high', 'low']
    if not all(col in df.columns for col in required_columns):
        raise ValueError(
            f"DataFrame должен содержать колонки: {required_columns}"
            )

    # Расчет SMA для high и low
    sma_high = calculate_sma(df['high'], period)
    sma_low = calculate_sma(df['low'], period)

    # Вычисление GannHiLo векторным способом
    gann_hilo = (
        (df['close'] > sma_high.shift(1)).astype(int) - 
        (df['close'] < sma_low.shift(1)).astype(int)
        )

    # Добавляем колонку Gann_HiLo в DataFrame
    df['Gann_HiLo'] = gann_hilo

    return df


# T3 --------------------------------------------------------------------------
def calculate_t3ma(df, period=8, b=0.618):
    """
    Рассчитывает индикатор T3MA и добавляет его как новую колонку в DataFrame.

    :param df: DataFrame с колонкой 'close'
    :param period: Период T3MA
    :param b: Параметр T3MA (значение сглаживания)
    :return: DataFrame с добавленной колонкой 'T3MA'
    """
    # Проверка на наличие колонки 'close'
    if 'close' not in df.columns:
        raise ValueError("DataFrame должен содержать колонку 'close'")

    close = df['close'].values

    # Коэффициенты
    b2 = b ** 2
    b3 = b2 * b
    c1 = -b3
    c2 = 3 * (b2 + b3)
    c3 = -3 * (2 * b2 + b + b3)
    c4 = 1 + 3 * b + b3 + 3 * b2
    n = max(1, 1 + 0.5 * (period - 1))
    w1 = 2 / (n + 1)
    w2 = 1 - w1

    # Создание временных массивов
    e1 = np.zeros_like(close)
    e2 = np.zeros_like(close)
    e3 = np.zeros_like(close)
    e4 = np.zeros_like(close)
    e5 = np.zeros_like(close)
    e6 = np.zeros_like(close)

    # Векторизованный расчет экспоненциальных сглаживаний
    e1[0] = close[0]
    for i in range(1, len(close)):
        e1[i] = w1 * close[i] + w2 * e1[i - 1]

    e2[0] = e1[0]
    for i in range(1, len(e1)):
        e2[i] = w1 * e1[i] + w2 * e2[i - 1]

    e3[0] = e2[0]
    for i in range(1, len(e2)):
        e3[i] = w1 * e2[i] + w2 * e3[i - 1]

    e4[0] = e3[0]
    for i in range(1, len(e3)):
        e4[i] = w1 * e3[i] + w2 * e4[i - 1]

    e5[0] = e4[0]
    for i in range(1, len(e4)):
        e5[i] = w1 * e4[i] + w2 * e5[i - 1]

    e6[0] = e5[0]
    for i in range(1, len(e5)):
        e6[i] = w1 * e5[i] + w2 * e6[i - 1]

    # Итоговый расчет T3
    t3 = c1 * e6 + c2 * e5 + c3 * e4 + c4 * e3

    # Добавление результата в DataFrame
    df['T3MA'] = t3
    return df


def run(df):
    # Добавляем колонку TVI
    df = add_tvi_column(df, r=12, s=12, u=5, point=0.1)
    # Добавляем GannHiLo индикатор
    df = add_gann_hilo_column_optimized(df, period=10)
    # Рассчитываем T3MA и добавляем в DataFrame
    df = calculate_t3ma(df, period=8, b=0.618)
    # Вычисление CCI с периодом 20
    df['CCI_20'] = ta.trend.cci(
        high=df['high'], low=df['low'], close=df['close'], window=20
        )
    
    # Добавляем новую колонку с сигналом TVI
    df['TVI_S'] = df['TVI'].diff().apply(lambda x: 1 if x > 0 else (-1 if x < 0 else 0))

    # Добавляем новую колонку с сигналом T3MA
    df['T3_S'] = df['T3MA'].diff().apply(lambda x: 1 if x > 0 else (-1 if x < 0 else 0))

    # Добавляем новую колонку с сигналом от CCI
    df['CCI_S'] = df['CCI_20'].apply(lambda x: 1 if x > 0 else (-1 if x < 0 else 0))

    df = df[['datetime', 'open', 'high', 'low', 'close', 
                'TVI_S', 'CCI_S', 'T3_S', 'Gann_HiLo']]

    df = df.copy()

    # Переименование колонок
    df.rename(
        columns={'TVI_S': 'tvi', 'CCI_S': 'cci', 'T3_S': 't3', 'Gann_HiLo': 'ghl'}, 
        inplace=True
        )
    return df
    

if __name__ == '__main__':  # Точка входа при запуске этого скрипта
    # Установить соединение с базой данных
    conn = sqlite3.connect(r'C:\Users\Alkor\gd\data_quote_db\RTS_futures_minute.db')

    # SQL-запрос для извлечения данных
    # query = "SELECT TRADEDATE, VOLUME FROM Minute"
    query = "SELECT * FROM Minute"

    # Загрузить данные в DataFrame
    df = pd.read_sql_query(query, conn)

    # Закрыть соединение
    conn.close()

    # Прописные в названиях колонок
    df.columns = map(str.lower, df.columns)

    # Преобразуем 'tradedate' в datetime формат, если это ещё не сделано
    df['tradedate'] = pd.to_datetime(df['tradedate'])

    # Фильтрация по условиям
    filtered_df = df[
        # # Все значения равны друг другу
        # (df['open'] == df['high']) & 
        # (df['open'] == df['low']) &
        # (df['open'] == df['close']) &
        # Минуты равны 59
        (df['tradedate'].dt.minute == 59) &  
        # Первая строка за дату
        (df.groupby(df['tradedate'].dt.date)['tradedate'].rank(method='first') == 1)  
    ]

    df = (
        df.merge(filtered_df, how='outer', indicator=True)
        .query('_merge == "left_only"').drop(columns=['_merge'])
        )
    
    # Убедимся, что колонка  tradedate имеет тип datetime
    df["tradedate"] = pd.to_datetime(df["tradedate"])

    # Устанавливаем  tradedate как индекс
    df.set_index("tradedate", inplace=True)

    # Агрегирование до 5-минутных баров
    df_m5 = df.resample("5min").agg({
        "open": "first",    # Первая цена в периоде
        "high": "max",      # Максимальная цена
        "low": "min",       # Минимальная цена
        "close": "last",    # Последняя цена
        "volume": "sum"    # Суммарный объём
    })

    # Сбрасываем индекс
    df_m5 = df_m5.reset_index()

    # Стереть строки с NaN
    df_m5.dropna(inplace=True)

    # Переиндексация
    df_m5 = df_m5.reset_index(drop=True)

    df = run(df_m5)
    print(df)
