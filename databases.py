import psycopg2
import mysql.connector
import datetime 

# import configparser

# config = configparser.RawConfigParser()
# filename = "dev.properties"

# config.read("./config/" + filename)

def _create_db_connetion(cfg, target):
    return {
        "psql": psycopg2.connect(
            host=cfg.get('PSQLSection', 'db.host'),
            user=cfg.get('PSQLSection', 'db.user'),
            password=cfg.get('PSQLSection', 'db.password'),
            database=cfg.get('PSQLSection', 'db.name')
        ),
        "mysql": mysql.connector.connect(
            host=cfg.get('MySQLSection', 'db.host'),
            user=cfg.get('MySQLSection', 'db.user'),
            password=cfg.get('MySQLSection', 'db.password'),
            database=cfg.get('MySQLSection', 'db.name')
        )
    }.get(target)

def _create_insert_statetment(table_name, columns_mapping):
    columns_str = ", ".join([k for k in columns_mapping])
    insert_into_statement = (
        f"INSERT INTO {table_name} ({columns_str}, createdat, updatedat) "
        f"VALUES ({'%s, ' * len(columns_mapping)}now(), now())"
    )
    return insert_into_statement

def _insert_to_database(record_tuple: tuple, insert_into_statement: str, cursor, conn):
    cursor.execute(
        insert_into_statement,
        record_tuple
    )
    conn.commit()

def insert_into_database(records, target, table_name, columns_mapping, config):
    conn = _create_db_connetion(config, target)
    insert_into_statement = _create_insert_statetment(
        table_name,
        columns_mapping
    )
    cursor = conn.cursor()
    [
        _insert_to_database(
            tuple(
                record.get(k)
                for k in record
            ),
            insert_into_statement,
            cursor,
            conn
        )
        for record in records
    ]
    cursor.close()
    conn.close()


# target = "psql"
# table_name = "crawlers.itp"

# columns_mapping = {
#     'kind': "str",
#     'berco': "str",
#     'navio': "str",
#     'viagem': "str",
#     'armador': "str",
#     'servico': "str",
#     'compr_m': "float",
#     'largura_m': "float",
#     'dt_abertura_do_gate': "datetime",
#     'dt_deadline': "datetime",
#     'dt_previsao_chegada': "datetime",
#     'dt_chegada': "datetime",
#     'dt_previsao_atracacao': "datetime",
#     'dt_atracacao': "datetime",
#     'dt_previsao_saida': "datetime",
#     'dt_saida': "datetime",
# }

# columns_str = ", ".join([k for k in columns_mapping])
# insert_into_statement = f"INSERT INTO {table_name} ({columns_str}, createdat, updatedat) VALUES ({'%s, ' * len(columns_mapping)}now(), now())"
# print(insert_into_statement)

# records = [{'kind': 'Navios Operados',
#   'berco': '1',
#   'navio': 'GSL MELINA-250W',
#   'viagem': 'GSMEL250W',
#   'armador': 'MSK',
#   'servico': 'ABAC',
#   'compr_m': '228.13',
#   'largura_m': '32.29',
#   'dt_abertura_do_gate': datetime.datetime(2022, 12, 7, 0, 0),
#   'dt_deadline': datetime.datetime(2022, 12, 13, 12, 0),
#   'dt_previsao_chegada': datetime.datetime(2022, 12, 13, 6, 0),
#   'dt_chegada': datetime.datetime(2022, 12, 13, 4, 30),
#   'dt_previsao_atracacao': datetime.datetime(2022, 12, 14, 12, 0),
#   'dt_atracacao': datetime.datetime(2022, 12, 14, 12, 18),
#   'dt_previsao_saida': datetime.datetime(2022, 12, 14, 23, 30),
#   'dt_saida': datetime.datetime(2022, 12, 14, 23, 42)}]

# def _insert_to_database(record_tuple: tuple, cursor, conn):
#     cursor.execute(
#         insert_into_statement,
#         record_tuple
#     )
#     conn.commit()

# cursor = conn.cursor()
# [
#     _insert_to_database(
#         tuple(
#             record.get(k)
#             for k in record
#         ),
#         cursor,
#         conn
#     )
#     for record in records
# ]
# cursor.close()
# conn.close()

# # if target == "mysql":
# #     conn.commit()
# # elif target == "psql":
# #     conn.commit()
# #     cursor.close()
# #     conn.close()

# mydb = mysql.connector.connect(
#     host=cfg.get('MySQLSection', 'db.host'),
#     user=cfg.get('MySQLSection', 'db.user'),
#     password=cfg.get('MySQLSection', 'db.password'),
#     database=cfg.get('MySQLSection', 'db.name')
# )

# mycursor = mydb.cursor()
# for s in range(len(ship_list)):
#     ship_list[s].convert_dt_objects('%Y-%m-%d %H:%M:%S')
    
#     sql = "INSERT INTO crawlers.itp (kind, berco, navio, viagem, armador, servico, compr_m, largura_m, dt_abertura_do_gate, dt_deadline, dt_previsao_chegada, dt_chegada, dt_previsao_atracacao, dt_atracacao, dt_previsao_saida, dt_saida, createdat, updatedat) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, now(), now())"

#     val = (
#         ship_list[s].kind,
#         ship_list[s].berco,
#         ship_list[s].navio,
#         ship_list[s].viagem,
#         ship_list[s].armador,
#         ship_list[s].servico,
#         ship_list[s].compr_m,
#         ship_list[s].largura_m,
#         ship_list[s].dt_abertura_do_gate_dtobj,
#         ship_list[s].dt_deadline_dtobj,
#         ship_list[s].dt_previsao_chegada_dtobj,
#         ship_list[s].dt_chegada_dtobj,
#         ship_list[s].dt_previsao_atracacao_dtobj,
#         ship_list[s].dt_atracacao_dtobj,
#         ship_list[s].dt_previsao_saida_dtobj,
#         ship_list[s].dt_saida_dtobj,
#     )
#     mycursor.execute(sql, val)
#     mydb.commit()


# # def _create_psql_connection(cfg):
# #     return psycopg2.connect(
# #         host=cfg.get('PSQLSection', 'db.host'),
# #         user=cfg.get('PSQLSection', 'db.user'),
# #         password=cfg.get('PSQLSection', 'db.password'),
# #         database=cfg.get('PSQLSection', 'db.database')
# #     )

# # def _create_mysql_connection(cfg):
# #     mysql.connector.connect(
# #         host=cfg.get('MySQLSection', 'db.host'),
# #         user=cfg.get('MySQLSection', 'db.user'),
# #         password=cfg.get('MySQLSection', 'db.password'),
# #         database=cfg.get('MySQLSection', 'db.name')
# #     )
