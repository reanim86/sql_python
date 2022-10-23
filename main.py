import psycopg2


def create_table():
    """
    Функция создает таблицы БД
    """
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS client (
                client_id SERIAL PRIMARY KEY,
                client_name VARCHAR(40) NOT NULL,
                client_last_name VARCHAR(40) NOT NULL,
                client_email VARCHAR(40)
            );
            CREATE TABLE IF NOT EXISTS tel (
                tel_id SERIAL PRIMARY KEY,
                client_telephone BIGINT UNIQUE
            );
            CREATE TABLE IF NOT EXISTS client_tel (
                client_id INTEGER REFERENCES client(client_id),
                tel_id INTEGER REFERENCES tel(tel_id),
                CONSTRAINT pk PRIMARY KEY (client_id, tel_id)
            );
        """)
        conn.commit()
    return

def add_client(name, surname, email='Незаполнен'):
    """
    Функция создает запись в таблице client
    :param name: столбец client_name
    :param surname: столбец client_last_name
    :param email: столбец client_email
    """
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO client(client_name, client_last_name, client_email)
            VALUES
                (%s, %s, %s);
        """, (name, surname, email))
        conn.commit()
    return

def add_tel(tel, name, surname):
    """
    Функция создает запись в таблице tel
    :param tel: столбец client_tel
    :param name: имя клиента
    :param surname: фамилия клиента
    """
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO tel(client_telephone)
            VALUES
                (%s);
        """, (tel, ))
        conn.commit()
    client = get_client_id(name, surname)
    telephone = get_tel_id(tel)
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO client_tel(client_id, tel_id)
            VALUES
                (%s, %s);
        """, (client, telephone))
        conn.commit()
    return

def get_client_id(name, surname):
    """
    Функция возвращает id клиента
    :param name: Имя клиента
    :param surname: Фамилия клиента
    """
    with conn.cursor() as cur:
        cur.execute("""
        SELECT client_id FROM client
        WHERE client_name = %s AND client_last_name = %s;
        """, (name, surname))
        return cur.fetchone()[0]

def get_tel_id(telphone):
    """
    Функция возвращает id телефона
    :param telphone: телефон
    """
    with conn.cursor() as cur:
        cur.execute("""
        SELECT tel_id FROM tel
        WHERE client_telephone = %s;
        """, (telphone,))
        return cur.fetchone()[0]

def update_client(name, surname, email, old_name, old_surname):
    """
    Функция изменяет данные о клиенте
    :param name: новое имя клиента
    :param surname: новая фамилия клиента
    :param email: новая эл. почта клиента
    :param old_name: старое имя клиента
    :param old_surname: старая фамилия клиента
    """
    id = get_client_id(old_name, old_surname)
    with conn.cursor() as cur:
        cur.execute("""
            UPDATE client
            SET client_name = %s, client_last_name = %s, client_email = %s
            WHERE client_id = %s;
            """, (name, surname, email, id))
        conn.commit()
    return

def del_tel(tel):
    """
    Функиця удялет запись в таблице tel и client_tel
    :param tel: номер телефона клиента
    """
    telephone = get_tel_id(tel)
    with conn.cursor() as cur:
        cur.execute("""
            DELETE FROM client_tel
            WHERE tel_id = %s;
        """, (telephone,))
        cur.execute("""
            DELETE FROM tel
            WHERE tel_id = %s;
        """, (telephone,))
        conn.commit()
    return

def del_client(name, surname):
    """
    Функция удялет запись в талбице client и client_tel если она есть
    :param name: имя клиента
    :param surname: фамилия клиента
    """
    client = get_client_id(name, surname)
    data_client = get_client_data(name, surname)
    tel = data_client[0][3]
    if tel is None:
        telephone = 0
    else:
        telephone = get_tel_id(tel)
    with conn.cursor() as cur:
        cur.execute("""
            DELETE FROM client_tel
            WHERE tel_id = %s;
            DELETE FROM client
            WHERE client_id = %s;
        """, (telephone, client))
        conn.commit()
    return

def get_client_data(name, surname, email='1', tel=1):
    with conn.cursor() as cur:
        cur.execute("""
            SELECT c.client_name, c.client_last_name, c.client_email, t.client_telephone FROM client c
            LEFT JOIN client_tel ct ON c.client_id = ct.client_id
            LEFT JOIN tel t ON ct.tel_id = t.tel_id
            WHERE 
                (c.client_name = (
                    SELECT c.client_name FROM client c
                    LEFT JOIN client_tel ct ON c.client_id = ct.client_id
                    LEFT JOIN tel t ON ct.tel_id = t.tel_id
                    WHERE c.client_name = %s
                    LIMIT 1)
                ) OR 
                (c.client_last_name = (
                    SELECT c.client_last_name FROM client c
                    LEFT JOIN client_tel ct ON c.client_id = ct.client_id
                    LEFT JOIN tel t ON ct.tel_id = t.tel_id
                    WHERE c.client_last_name = %s
                    LIMIT 1)
                ) OR 
                (c.client_email = (
                    SELECT c.client_email FROM client c
                    LEFT JOIN client_tel ct ON c.client_id = ct.client_id
                    LEFT JOIN tel t ON ct.tel_id = t.tel_id
                    WHERE c.client_email = %s
                    LIMIT 1)
                    ) OR 
                (t.client_telephone = %s);
        """, (name, surname, email, tel))
        return cur.fetchall()


with psycopg2.connect(database='clients_db', user='postgres', password='Tehn89tehn') as conn:
    if __name__ == '__main__':
        create_table()
        add_client('Nick', 'Nicolanson')
        add_tel(89305678025, 'Nick', 'Nicolanson')
        update_client('Ale', 'M', '123@ya.ru', 'Nick', 'Nicolanson')
        del_tel(89305678025)
        del_client('Ale', 'M')
        print(get_client_data('Roma', 'Ivano', 'ri@cloud.', 89195679099))
conn.close()
