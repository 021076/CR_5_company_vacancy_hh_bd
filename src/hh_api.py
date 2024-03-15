# import json
import requests


class APIhh:

    @staticmethod
    def get_employers():
        """ Получение базы работодателей с платформы hh.ru """
        name_employers = (
            "Яндекс.Такси", "X5 Tech", "Инглекс", "Сбербанк-Сервис", "Акционерное общество «Энергоспецмонтаж»",
            "Лаборатория Касперского", "1С-Битрикс Казахстан", "РЖД-ЗДОРОВЬЕ", "Центр винного туризма Абрау Дюрсо",
            "Инновационный центр КАМАЗ")
        list_employers_dict = []
        for employer in name_employers:
            params = {"text": employer, "page": 0, "per_page": 20, "only_with_vacancies": True}
            employers_response = requests.get("https://api.hh.ru/employers/", params)
            # print(json.dumps(employers_response.json(), indent=2, ensure_ascii=False))
            for dict_employer in employers_response.json()["items"]:
                list_employers_dict.append(dict_employer)
        return list_employers_dict

    @staticmethod
    def get_vacancies(id_employer: str):
        """ Получение базы вакансий по id работодателя """
        list_vacancies_dict = []
        url = "https://api.hh.ru/vacancies?employer_id=" + id_employer
        params = {"page": 0, "per_page": 100, "only_with_salary": True}
        vacancies_response = requests.get(url, params)
        # print(json.dumps(vacancies_response.json(), indent=2, ensure_ascii=False))
        for dict_vacancy in vacancies_response.json()["items"]:
            list_vacancies_dict.append(dict_vacancy)
        return list_vacancies_dict
