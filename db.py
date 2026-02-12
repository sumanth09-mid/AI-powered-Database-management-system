import psycopg2


def get_connection():

    try:
        return psycopg2.connect(
            dbname="company_db",
            user='sumanth',
            password="Itwasntme@11",
            host="localhost",
            port="5432"
        )
    except:
        return False
