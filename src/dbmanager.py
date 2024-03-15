import psycopg2


class DBManager:
    """Метод для работы с базоми данных в СУБД PostgreSQL"""

    @staticmethod
    def create_db(db_name: str):
        """Метод создания базы данных"""
        conn_params = psycopg2.connect(host="127.0.0.1", user="postgres", password="bd_pyt", client_encoding="utf-8")
        cursor = conn_params.cursor()
        # автокоммит
        conn_params.autocommit = True
        try:
            # команда для создания базы данных
            cursor.execute("CREATE DATABASE " + db_name)
            print(f"\nБаза данных {db_name} успешно создана")
        except psycopg2.ProgrammingError as err:
            print(err)
        finally:
            cursor.close()
            conn_params.close()

    @staticmethod
    def create_table(db_name: str, table_name: str, param_table: str):
        """Метод для создания таблицы базы данных"""
        conn_params = psycopg2.connect(host="127.0.0.1", user="postgres",
                                       password="bd_pyt", database=db_name, client_encoding="utf-8")
        cursor = conn_params.cursor()
        conn_params.autocommit = True
        try:
            cursor.execute(f"CREATE TABLE {table_name} ({param_table})")
            print(f"Таблица {table_name} успешно создана")
        except psycopg2.ProgrammingError as err:
            print(err)
        finally:
            cursor.close()
            conn_params.close()

    @staticmethod
    def insert_table(db_name: str, table_name: str, param_insert):
        """Метод для наполнения таблицы базы данных"""
        conn_params = psycopg2.connect(host="127.0.0.1", database=db_name, user="postgres",
                                       password="bd_pyt", client_encoding="utf-8")
        cursor = conn_params.cursor()
        conn_params.autocommit = True
        val = ("%s, " * len(param_insert))[:-2]
        try:
            cursor.execute(f"INSERT INTO {table_name} VALUES ({val})", param_insert)
        except psycopg2.ProgrammingError as err:
            print(err)
        finally:
            cursor.close()
            conn_params.close()

    @staticmethod
    def drop_bd(db_name: str):
        """Метод для удаления базы данных"""
        conn_params = psycopg2.connect(host="127.0.0.1", database="postgres", user="postgres",
                                       password="bd_pyt", client_encoding="utf-8")
        cursor = conn_params.cursor()
        conn_params.autocommit = True
        try:
            cursor.execute(f"DROP DATABASE {db_name} with(Force)")
            print(f"База данных {db_name} удалена")
        except psycopg2.ProgrammingError as err:
            print(err)
        finally:
            cursor.close()
            conn_params.close()

    # @staticmethod
    # def drop_table(db_name: str, table_name):
    #     """Метод для удаления таблицы в базе данных"""
    #     conn_params = psycopg2.connect(host="127.0.0.1", database=db_name, user="postgres",
    #                                    password="bd_pyt", client_encoding="utf-8")
    #     cursor = conn_params.cursor()
    #     conn_params.autocommit = True
    #     try:
    #         cursor.execute("DROP TABLE " + table_name + " CASCADE")
    #         print(f"Таблица {table_name} в базу данных {db_name} удалена")
    #     except psycopg2.ProgrammingError as err:
    #         print(err)
    #     finally:
    #         cursor.close()
    #         conn_params.close()
    #
    # @staticmethod
    # def truncate_table(db_name: str, table_name):
    #     """Метод для очищения таблицы"""
    #     conn_params = psycopg2.connect(host="127.0.0.1", database=db_name, user="postgres",
    #                                    password="bd_pyt", client_encoding="utf-8")
    #     cursor = conn_params.cursor()
    #     conn_params.autocommit = True
    #     try:
    #         cursor.execute(f"TRUNCATE TABLE {table_name} CASCADE")
    #     except psycopg2.ProgrammingError as err:
    #         print(err)
    #     finally:
    #         cursor.close()
    #         conn_params.close()

    @staticmethod
    def db_exists(db_name: str):
        """Проверка, что база данных существует"""
        try:
            psycopg2.connect(host="127.0.0.1", database=db_name, user="postgres",
                             password="bd_pyt", client_encoding="utf-8")
            return True
        except:
            return False

    @staticmethod
    def get_count_table(db_name: str, table_name: str):
        """Проверка наличия записей в таблице"""
        conn_params = psycopg2.connect(host="127.0.0.1", database=db_name, user="postgres",
                                       password="bd_pyt", client_encoding="utf-8")
        try:
            with conn_params:
                with conn_params.cursor() as cursor:
                    cursor.execute(f"SELECT count(*) FROM {table_name}")
                    rows = cursor.fetchone()
                    return rows
        finally:
            conn_params.close()

    @staticmethod
    def get_schema_bd(db_name: str):
        """Вывод списка всех таблиц в базе данных"""
        conn_params = psycopg2.connect(host="127.0.0.1", database=db_name, user="postgres",
                                       password="bd_pyt", client_encoding="utf-8")
        try:
            with conn_params:
                with conn_params.cursor() as cursor:
                    cursor.execute(f"SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")
                    rows = cursor.fetchall()
                    tables_bd = []
                    for row in rows:
                        tables_bd.append(row[0])
            return tables_bd
        finally:
            conn_params.close()

    @staticmethod
    def get_companies_and_vacancies_count():
        """Получает список всех компаний и количество вакансий у каждой компании из БД employers_vacancies_hh"""
        conn_params = psycopg2.connect(host="127.0.0.1", database="employers_vacancies_hh", user="postgres",
                                       password="bd_pyt", client_encoding="utf-8")
        try:
            with conn_params:
                with conn_params.cursor() as cursor:
                    cursor.execute("select name_employer, open_vacancies from employers_hh")
                    rows = cursor.fetchall()
                    return rows
        finally:
            conn_params.close()

    @staticmethod
    def get_all_vacancies():
        """Получает список всех вакансий с указанием названия компании,
        названия вакансии и зарплаты и ссылки на вакансию из БД employers_vacancies_hh"""
        conn_params = psycopg2.connect(host="127.0.0.1", database="employers_vacancies_hh", user="postgres",
                                       password="bd_pyt", client_encoding="utf-8")
        try:
            with conn_params:
                with conn_params.cursor() as cursor:
                    cursor.execute(f"select name_employer, name_vacancy, salary, url_vacancy "
                                   f"from employers_hh join vacancies_hh using(id_employer)")
                    rows = cursor.fetchall()
                    return rows
        finally:
            conn_params.close()

    @staticmethod
    def get_avg_salary():
        """Получает среднюю зарплату по вакансиям из БД employers_vacancies_hh"""
        conn_params = psycopg2.connect(host="127.0.0.1", database="employers_vacancies_hh", user="postgres",
                                       password="bd_pyt", client_encoding="utf-8")
        try:
            with conn_params:
                with conn_params.cursor() as cursor:
                    cursor.execute("select name_vacancy, AVG(salary) from vacancies_hh GROUP BY(name_vacancy)")
                    rows = cursor.fetchall()
                    return rows
        finally:
            conn_params.close()

    @staticmethod
    def get_vacancies_with_higher_salary():
        """Получает список всех вакансий, у которых зарплата выше средней
        по всем вакансиям из БД employers_vacancies_hh"""
        conn_params = psycopg2.connect(host="127.0.0.1", database="employers_vacancies_hh", user="postgres",
                                       password="bd_pyt", client_encoding="utf-8")
        try:
            with conn_params:
                with conn_params.cursor() as cursor:
                    cursor.execute(f"select name_vacancy, salary from vacancies_hh "
                                   f"where salary > (select AVG(salary) from vacancies_hh) order by salary desc")
                    rows = cursor.fetchall()
                    return rows
        finally:
            conn_params.close()

    @staticmethod
    def get_vacancies_with_keyword(word: str):
        """Получает список всех вакансий, в названии которых содержатся переданные в метод слова,
        например python из БД employers_vacancies_hh"""
        conn_params = psycopg2.connect(host="127.0.0.1", database="employers_vacancies_hh", user="postgres",
                                       password="bd_pyt", client_encoding="utf-8")

        try:
            with conn_params:
                with conn_params.cursor() as cursor:
                    cursor.execute(f"select count(*) from vacancies_hh where name_vacancy like '%{word.capitalize()}%' "
                                   f"or name_vacancy like '%{word.upper()}%' or name_vacancy like '%{word.lower()}%'")
                    rows = cursor.fetchone()
                    for row in rows:
                        try:
                            if row == 0:
                                raise TypeError
                        except:
                            print("Вакансии не найдены")
                        cursor.execute(
                            f"select name_vacancy from vacancies_hh where name_vacancy like '%{word.capitalize()}%' "
                            f"or name_vacancy like '%{word.upper()}%' or name_vacancy like '%{word.lower()}%'")
                        rows = cursor.fetchall()
                        return rows
        finally:
            conn_params.close()
