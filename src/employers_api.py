import json
import requests


class EmployersHHAPI:
    @staticmethod
    def get_employers():
        """ Получение базы работодателей с платформы hh.ru из числа топ лучших работодателей
         по версии https://www.im-konsalting.ru/blog/20-luchshih-rabotodatelej-rossii/"""
        name_employers = (
            # "Русагро", "Яндекс", "Ростелеком", "X5", "Газпром", "Сбербанк", "Росатом", "Лаборатория Касперского", "Аэрофолот", "1С")
        "Яндекс.Такси", "X5 Tech", "Инглекс", "Сбербанк-Сервис", "Акционерное общество «Энергоспецмонтаж»", "Лаборатория Касперского", "1С-Битрикс Казахстан", "РЖД-ЗДОРОВЬЕ", "Центр винного туризма Абрау Дюрсо", "Инновационный центр КАМАЗ")
        list_employers_dict = []
        for employer in name_employers:
            params = {"text": employer, "page": 0, "per_page": 100, "only_with_vacancies": True}
            employers_response = requests.get("https://api.hh.ru/employers/", params)
            for dict_employer in employers_response.json()["items"]:
                list_employers_dict.append(dict_employer)
        # return json.dumps(list_employers_dict, indent=2, ensure_ascii=False)
        print(json.dumps(list_employers_dict, indent=2, ensure_ascii=False))
        with open("employers_hh.json", 'w+', encoding='utf-8') as file:
            json.dump(list_employers_dict, file, ensure_ascii=False)  # строкой


f = EmployersHHAPI.get_employers()

# def read_from_json(self):
#     """Чтение данных из файла json"""
#     try:
#         open(self.json_file)
#     except FileNotFoundError:
#         raise FileNotFoundError(f"Файл {self.json_file} не найден")
#     else:
#         with open(self.json_file, "r+", encoding='utf-8') as file:
#             return json.load(file)
