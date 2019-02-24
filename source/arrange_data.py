import pandas as pd
import os
import sqlite3


db_path = '../db/Climate_Change_3.db'
rcps = ['RCP26', 'RCP45', 'RCP60', 'RCP85']

def create_db(tables):

    con = sqlite3.connect(db_path)
    cur = con.cursor()

    for table_name in tables:

        query = """
            CREATE TABLE IF NOT EXISTS {}(
            date DATETIME NOT NULL,
            station TEXT NOT NULL,
            sensor TEXT NOT NULL,
            model TEXT NOT NULL,
            value REAL NOT NULL
            );
            """.format(table_name)

        cur.execute(query)

        query = """
            CREATE UNIQUE INDEX IF NOT EXISTS idx_{0} ON {0} (date, station, sensor, model);
            """.format(table_name)
        cur.execute(query)

    con.close()


def arrange_source():
    path_data = '../data'

    dt_data = {
        'Precipitacion': {
            'sensor': '0240',
            'data': ['Datos_Ajustados', 'Datos_Originales']
        },
        'T_Media': {
            'sensor': '0068',
            'data': ['Datos_Ajustados', 'Datos_Originales']
        }
    }

    con = sqlite3.connect(db_path)
    cur = con.cursor()

    for folder_variable in dt_data:
        print(folder_variable)
        path_var = '{}/{}'.format(path_data, folder_variable)
        sensor = dt_data[folder_variable]['sensor']

        for data_type in dt_data[folder_variable]['data']:
            path_type = '{}/{}'.format(path_var, data_type)

            for rcp in rcps:
                path_rcp = '{}/{}'.format(path_type, rcp)
                zones = os.listdir(path_rcp)

                for zone in zones:
                    path_zone = '{}/{}'.format(path_rcp, zone)
                    stations = os.listdir(path_zone)

                    for station in stations:
                        print('{}>{}>{}>{}>{}'.format(folder_variable, data_type, rcp, zone, station))
                        path_station = '{}/{}'.format(path_zone, station)
                        station_code = '{}0'.format(station.split('-')[0])

                        df_station = pd.read_csv(path_station, sep='\t')
                        df_station['Fecha'] = df_station['Anyo'].astype(str).str.cat(
                            [df_station['Mes'].map('{:02}-01'.format)], sep='-')
                        df_station.drop(['Anyo', 'Mes'], axis=1, inplace=True)
                        df_station.set_index('Fecha', inplace=True)
                        df_station.columns.name = 'Modelo'
                        df_station.sort_index(inplace=True)

                        df_models = df_station.unstack().reset_index()
                        df_models.dropna(inplace=True)

                        to_db = [
                            (
                                df_models.loc[i, 'Fecha'],
                                station_code,
                                sensor,
                                df_models.loc[i, 'Modelo'],
                                df_models.loc[i, 0]
                            )

                            for i in df_models.index
                        ]

                        query = """
                                    INSERT OR REPLACE INTO {}(date, station, sensor, model, value)
                                    VALUES(?, ?, ?, ?, ?)
                                    """.format(rcp)

                        cur.executemany(query, to_db)
                        con.commit()

    con.close()


def arrange_ensemble():
    path_data = '../data'

    dt_data = {
        'Precipitacion': {
            'sensor': '0240',
            'data': ['Series']
        },
        'T_Media': {
            'sensor': '0068',
            'data': ['Series']
        }
    }

    con = sqlite3.connect(db_path)
    cur = con.cursor()

    for folder_variable in dt_data:
        print(folder_variable)
        path_var = '{}/{}'.format(path_data, folder_variable)
        sensor = dt_data[folder_variable]['sensor']

        for data_type in dt_data[folder_variable]['data']:
            path_type = '{}/{}'.format(path_var, data_type)
            zones = os.listdir(path_type)

            for zone in zones:
                path_zone = '{}/{}'.format(path_type, zone)
                stations = os.listdir(path_zone)

                for station in stations:
                    print('{}>{}>{}>{}'.format(folder_variable, data_type, zone, station))
                    path_station = '{}/{}'.format(path_zone, station)
                    station_code = '{}0'.format(station.split('-')[0])

                    df_station = pd.read_csv(path_station, sep='\t')
                    df_station['Fecha'] = df_station['Anyo'].astype(str).str.cat(
                        [df_station['Mes'].map('{:02}-01'.format)], sep='-')
                    df_station.drop(['Anyo', 'Mes'], axis=1, inplace=True)
                    df_station.set_index('Fecha', inplace=True)
                    df_station.sort_index(inplace=True)

                    for rcp in rcps:
                        df_rcp = df_station[[rcp]].copy()
                        df_rcp.dropna(inplace=True)
                        df_rcp.reset_index(inplace=True)

                        to_db = [
                            (
                                df_rcp.loc[i, 'Fecha'],
                                station_code,
                                sensor,
                                'Ensamble',
                                df_rcp.loc[i, rcp]
                            )

                            for i in df_rcp.index
                        ]

                        query = """
                                                INSERT OR REPLACE INTO {}(date, station, sensor, model, value)
                                                VALUES(?, ?, ?, ?, ?)
                                                """.format(rcp)

                        cur.executemany(query, to_db)
                        con.commit()

    con.close()


if __name__ == '__main__':
    create_db(rcps)
    arrange_source()
    arrange_ensemble()
    pass
