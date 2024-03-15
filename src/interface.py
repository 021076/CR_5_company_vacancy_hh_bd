from src.dbmanager import DBManager
from src.hh_api import APIhh


def interface():
    # задаем названия базы и таблиц
    db_name_hh = "employers_vacancies_hh"
    employers_hh_table = "employers_hh"
    vacancies_hh_table = "vacancies_hh"
    # получение данных о работодателях с сайта hh.ru
    list_employers_dict = APIhh.get_employers()
    # формирование списка id работодателей для поиска вакансий
    list_id = []
    for emp in list_employers_dict:
        list_id.append(str(emp["id"]))

    # создание таблицы employers_hh
    def create_employers_hh():
        param_table_employers = (f"id_employer int PRIMARY KEY, name_employer text, open_vacancies int, "
                                 f"url_employer varchar(40), url_vacancies varchar(60)")
        DBManager.create_table(db_name_hh, employers_hh_table, param_table_employers)

    # создание таблицы vacancies_hh
    def create_vacancies_hh():
        param_table_vacancies = (
            f"id_vacancy int PRIMARY KEY, id_employer int REFERENCES employers_hh(id_employer) NOT NULL, "
            f"name_vacancy text, area_name varchar(100), "
            f"salary real, salary_currency varchar(3), "
            f"type_name varchar(10), "
            f"published_at date, created_at date, archived boolean, url_vacancy varchar(60), "
            f"snippet_requirement text, snippet_responsibility text, "
            f"schedule_name varchar(30), "
            f"experience_name varchar(25), employment_name varchar(25)")
        DBManager.create_table(db_name_hh, vacancies_hh_table, param_table_vacancies)

    # наполнение таблицы employers_hh
    def insert_table_employers():
        for e in list_employers_dict:
            id_employer = int(e["id"])
            name_employer = str(e["name"])
            open_vacancies = int(e["open_vacancies"])
            url_employer = str(e["url"])
            url_vacancies = str(e["vacancies_url"])
            DBManager.insert_table(db_name_hh, employers_hh_table,
                                   (id_employer, name_employer, open_vacancies, url_employer, url_vacancies))
        print(f"Наполнение таблицы {employers_hh_table} данными успешно выполнено")

    # наполнение таблицы vacancies_hh
    def insert_table_vacancies():
        for vac in list_id:
            for v in APIhh.get_vacancies(vac):
                id_vacancy = int(v["id"])
                id_employer = int(v["employer"]["id"])
                name_vacancy = str(v["name"])
                area_name = str(v["area"]["name"])
                if v["salary"]["from"] and v["salary"]["to"]:
                    salary = v["salary"]["to"]
                elif not v["salary"]["from"]:
                    salary = v["salary"]["to"]
                elif not v["salary"]["to"]:
                    salary = v["salary"]["from"]
                salary_currency = str(v["salary"]["currency"])
                type_name = str(v["type"]["name"])
                published_at = (v["published_at"])
                created_at = (v["created_at"])
                archived = bool(v["archived"])
                url_vacancy = str(v["url"])
                snippet_requirement = (v["snippet"]["requirement"])
                snippet_responsibility = (v["snippet"]["responsibility"])
                schedule_name = str(v["schedule"]["name"])
                experience_name = str(v["experience"]["name"])
                employment_name = str(v["employment"]["name"])
                DBManager.insert_table(db_name_hh, vacancies_hh_table,
                                       (id_vacancy, id_employer, name_vacancy, area_name, salary,
                                        salary_currency, type_name, published_at, created_at, archived, url_vacancy,
                                        snippet_requirement, snippet_responsibility, schedule_name,
                                        experience_name, employment_name))
        print(f"Наполнение таблицы {vacancies_hh_table} данными успешно выполнено")

    # информация по найденной базе
    def info_db():
        tables_bd = DBManager.get_schema_bd(db_name_hh)
        if len(tables_bd) > 0:
            print(f"Таблицы базы данных '{db_name_hh}': {tables_bd}")
            for tab in tables_bd:
                for row in DBManager.get_count_table(db_name_hh, tab):
                    if row > 0:
                        print(f"В таблице {tab} {row} строк данных")
                    else:
                        print(f"В таблице {tab} данных нет")

    # меню для работы с выборками
    def menu_select():
        print(f"Доcтупны следующие выборки из базы данных {db_name_hh}: \n"
              f"1. Cписок всех компаний и количество вакансий у каждой компании\n"
              f"2. Cписок всех вакансий с указанием названия компании, "
              f"названия вакансии, зарплаты и ссылки на вакансию\n"
              f"3. Cредняя зарплата по вакансиям\n"
              f"4. Список всех вакансий, у которых зарплата выше средней по всем вакансиям\n"
              f"5. Список всех вакансий, поиск по ключевому слову в названии вакансии")
        while True:
            num_select = str(input("Выберите номер выборки: "))
            if num_select == "1":
                print("Cписок всех компаний и количество вакансий у каждой компании:")
                rows = DBManager.get_companies_and_vacancies_count()
                for row in rows:
                    print(row)
                break
            elif num_select == "2":
                print(
                    "Cписок всех вакансий с указанием названия компании, "
                    "названия вакансии, зарплаты и ссылки на вакансию:")
                rows = DBManager.get_all_vacancies()
                for row in rows:
                    print(row)
                break
            elif num_select == "3":
                print("Cредняя зарплата по вакансиям\n")
                rows = DBManager.get_vacancies_with_higher_salary()
                for row in rows:
                    print(row)
                break
            elif num_select == "4":
                print("Список всех вакансий, у которых зарплата выше средней по всем вакансиям:")
                rows = DBManager.get_avg_salary()
                for row in rows:
                    print(row)
                break
            elif num_select == "5":
                keyword = input("Введите ключевое слово для поиска\n")
                print(f"Список всех вакансий, поиск по ключевому слову '{keyword}' в названии вакансии: ")
                rows = DBManager.get_vacancies_with_keyword(keyword)
                for row in rows:
                    print(row)
                break
            # elif str(num_select):
            #     print("Некорректный номер")
            #     continue
            else:
                print("Некорректный номер")
                continue

    # работа с выборками
    def select_data():
        while True:
            menu_select()
            rev_select = str(input("Еще одну выборку (y/n)?: "))
            if rev_select == "y":
                continue
            else:
                print("Выход")
                break

    # основной алгоритм
    # проверка существования базы данных
    if DBManager.db_exists(db_name_hh):
        print(f"Внимание! База данных {db_name_hh} уже существует'")
        # информация по найденной базе данных
        info_db()
        print("Выберите какое действие выполнить дальше:")
        work_bd = int(input("- СОЗДАТЬ новую базу данных, нажмите 1\n"
                            "- ПРОДОЛЖИТЬ работу в базе данных без изменений, нажмите 2\n"))
        if work_bd == 1:
            # удаление базы данных
            DBManager.drop_bd(db_name_hh)
            # создание базы данных
            DBManager.create_db(db_name_hh)
            # создание таблиц
            create_employers_hh()
            create_vacancies_hh()
            # наполнение таблиц
            insert_table_employers()
            insert_table_vacancies()
            # информация по новой базе данных
            info_db()
            # работа с выборками
            select_data()
        elif work_bd == 2:
            # работа с выборками
            select_data()
    else:
        # создание базы данных
        DBManager.create_db(db_name_hh)
        # создание таблиц
        create_employers_hh()
        create_vacancies_hh()
        # наполнение таблиц
        insert_table_employers()
        insert_table_vacancies()
        # информация по новой базе данных
        info_db()
        # работа с выборками
        select_data()
