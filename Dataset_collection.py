import json
import re
from random import uniform, choice
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.firefox.options import Options
from selenium.webdriver.firefox.service import Service
from time import sleep
from tqdm import tqdm
import os
import pickle
import logging
from urllib.parse import quote
from datetime import datetime

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("scraping.log", encoding='utf-8'),
        logging.StreamHandler()
    ]
)

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:89.0) Gecko/20100101 Firefox/89.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
]

# Константы
TOTAL_ARTICLES = 10000  # Сколько всего статей хотим собрать
MIN_TIME_SLEEP = 1      # Минимальная задержка между запросами
MAX_TIME_SLEEP = 2      # Максимальная задержка между запросами
OUTPUT_FILE = "ncr_russia_dataset.json"
MAX_RETRIES = 3         # Максимум попыток загрузки страницы

# Список запросов по НЦР (оставлен без изменений)
goals_and_tasks = {
    "1_Сохранение_насел-развитие_здоровье_благополучие_семья": [
        "повышение суммарного коэффициента рождаемости до 1,6 к 2030 году россия",
        "увеличение ожидаемой продолжительности жизни до 78 лет к 2030 году россия",
        "рост суммарного коэффициента рождаемости третьих и последующих детей россия",
        "снижение дифференциации ожидаемой продолжительности жизни россия",
        "снижение суммарной продолжительности временной нетрудоспособности россия",
        "уровень удовлетворенности условиями для занятий физической культурой россия",
        "услуги долговременного ухода для граждан пожилого возраста россия",
        "условия для медицинской реабилитации участников специальной военной операции россия",
        "цифровая платформа для сохранения здоровья россия",
        "снижение уровня бедности ниже 7 процентов к 2030 году россия",
        "снижение коэффициента Джини до 0,37 к 2030 году россия",
        "рост минимального размера оплаты труда до 35 тыс рублей к 2030 году россия",
        "новые системы оплаты труда работников государственных организаций россия",
        "здоровье россии",
    ],
    "2_Реализация_потенциала_таланты_патриотизм": [
        "условия для воспитания патриотичной и социально ответственной личности россия",
        "увеличение численности иностранных студентов до 500 тыс к 2030 году россия",
        "доля молодых людей в программах профессионального и патриотического воспитания россия",
        "доля молодых людей верящих в возможности самореализации в России",
        "молодые люди вовлеченные в добровольческую деятельность россия",
        "система выявления и поддержки талантов детей и молодежи россия",
        "традиционные духовно-нравственные ценности в проектах культуры россия",
        "удовлетворенность граждан работой организаций культуры россия",
        "система профессионального развития педагогических работников россия",
        "образование россии",
    ],
    "3_Комфортная_безопасная_среда": [
        "улучшение качества среды для жизни в опорных населенных пунктах россия",
        "обеспечение граждан жильем 33 кв метра на человека к 2030 году россия",
        "обновление жилищного фонда на 20 процентов к 2030 году россия",
        "сокращение непригодного для проживания жилищного фонда россия",
        "повышение доступности жилья на первичном рынке россия",
        "благоустройство 30 тыс общественных территорий к 2030 году россия",
        "модернизация коммунальной инфраструктуры для 20 млн человек россия",
        "строительство 2 тыс объектов питьевого водоснабжения к 2030 году россия",
        "рост энергетической эффективности в жилищно-коммунальном хозяйстве россия",
        "доля общественного транспорта со сроком эксплуатации не старше нормативного россия",
        "доля автомобильных дорог соответствующих нормативным требованиям россия",
        "снижение смертности в результате дорожно-транспортных происшествий россия",
        "рост авиационной подвижности населения на 50 процентов к 2030 году россия",
        "капитальный ремонт зданий дошкольных и общеобразовательных организаций россия",
        "программа социальной газификации 1,6 млн домовладений к 2030 году россия",
        "оснащение 900 центров воспроизведения аудиовизуального контента россия",
        "инфраструктура россии",
    ],
    "4_Экологическое_благополучие": [
        "экономика замкнутого цикла сортировка 100 процентов твердых отходов россия",
        "снижение выбросов опасных загрязняющих веществ в городах россия",
        "ликвидация 50 опасных объектов накопленного вреда к 2030 году россия",
        "снижение объема неочищенных сточных вод в водные объекты россия",
        "сохранение лесов и биологического разнообразия россия",
        "экология россии",
    ],
    "5_Устойчивая_динамичная_экономика": [
        "рост валового внутреннего продукта выше среднемирового россия",
        "снижение доли импорта в структуре валового внутреннего продукта россия",
        "увеличение объема инвестиций в основной капитал на 60 процентов россия",
        "устойчивый рост доходов населения и пенсионного обеспечения россия",
        "рост дохода работников малого и среднего предпринимательства россия",
        "рост капитализации фондового рынка до 66 процентов ВВП к 2030 году россия",
        "вхождение России в топ-25 стран по плотности роботизации",
        "повышение производительности труда в несырьевых отраслях россия",
        "система подготовки кадров для приоритетных отраслей экономики россия",
        "освоение нескольких квалификаций студентами россия",
        "условия для профессионального развития работающих граждан россия",
        "снижение разрыва в бюджетной обеспеченности субъектов РФ россия",
        "доля туристской отрасли в ВВП до 5 процентов к 2030 году россия",
        "рост экспорта несырьевых неэнергетических товаров россия",
        "рост производства продукции агропромышленного комплекса россия",
        "рост экспорта продукции агропромышленного комплекса россия",
        "сеть устойчивых партнерств с иностранными государствами россия",
        "рост экспорта туристских услуг в три раза россия",
        "увеличение перевозок по международным транспортным коридорам россия",
        "доля креативных индустрий в экономике россия",
        "программы адаптации к изменениям климата россия",
        "национальная система мониторинга климатически активных веществ россия",
        "экономика россии",
    ],
    "6_Технологическое_лидерство": [
        "технологическая независимость в биоэкономике россия",
        "технологическая независимость в беспилотных авиационных системах россия",
        "технологическая независимость в средствах производства россия",
        "технологическая независимость в транспортной мобильности россия",
        "технологическая независимость в экономике данных россия",
        "технологическая независимость в искусственном интеллекте россия",
        "технологическая независимость в новых материалах россия",
        "технологическая независимость в космических технологиях россия",
        "технологическая независимость в новых энергетических технологиях россия",
        "рост валовой добавленной стоимости в обрабатывающей промышленности россия",
        "вхождение России в топ-10 стран по объему научных исследований",
        "увеличение внутренних затрат на исследования и разработки россия",
        "рост доли отечественных высокотехнологичных товаров россия",
        "рост выручки малых технологических компаний россия",
        "технологии россии",
    ],
    "7_Цифровая_трансформация": [
        "достижение цифровой зрелости государственного управления россия",
        "формирование рынка данных и их вовлечение в хозяйственный оборот россия",
        "доступ домохозяйств к высокоскоростному интернету россия",
        "рост инвестиций в отечественные IT решения россия",
        "переход организаций на российское программное обеспечение россия",
        "использование российского ПО в государственных органах россия",
        "предоставление государственных услуг в электронной форме россия",
        "система подбора кадров для органов власти на цифровой платформе россия",
        "повышение удовлетворенности граждан качеством работы госслужащих россия",
        "противодействие преступлениям с использованием IT россия",
        "обеспечение сетевого суверенитета и информационной безопасности россия",
        "цифровизация россии",
    ]
}

class TextCollector:
    def __init__(self, base_url, min_time_sleep, max_time_sleep, headless=True):
        # Инициализация сборщика: настройка базового URL и задержек
        self.base_url = base_url
        self.min_time_sleep = min_time_sleep
        self.max_time_sleep = max_time_sleep
        self.headless = headless
        self.cookies_file = "cookies.pkl"
        self.setup_driver()

    def setup_driver(self):
        # Настройка браузера Firefox: выбор User-Agent, отключение WebDriver обнаружения
        options = Options()
        if self.headless:
            options.add_argument('--headless')
        options.binary_location = r"C:\Program Files\Mozilla Firefox\firefox.exe"
        options.set_preference("general.useragent.override", choice(USER_AGENTS))
        options.set_preference("dom.webdriver.enabled", False)
        service = Service(executable_path=r"C:\Users\Super\geckodriver.exe")
        self.driver = webdriver.Firefox(options=options, service=service)
        self.load_cookies()

    def close(self):
        # Закрытие браузера и сохранение cookies
        self.save_cookies()
        try:
            self.driver.quit()
        except:
            pass

    def save_cookies(self):
        # Сохранение cookies в файл для повторного использования
        try:
            with open(self.cookies_file, "wb") as f:
                pickle.dump(self.driver.get_cookies(), f)
        except Exception as e:
            logging.error(f"Ошибка сохранения cookies: {e}")

    def load_cookies(self):
        # Загрузка cookies из файла, если они есть
        if os.path.exists(self.cookies_file):
            try:
                self.driver.get(self.base_url)
                with open(self.cookies_file, "rb") as f:
                    for cookie in pickle.load(f):
                        self.driver.add_cookie(cookie)
                self.driver.refresh()
            except Exception as e:
                logging.error(f"Ошибка загрузки cookies: {e}")

    def fetch_content(self, url, retries=MAX_RETRIES):
        # Загрузка страницы с повторными попытками в случае ошибки
        for attempt in range(retries):
            try:
                logging.info(f"Загружаем страницу: {url}")
                self.driver.get(url)
                sleep(uniform(0.5, 1))
                html = self.driver.page_source
                if "Вы точно человек?" in html or "503 Service" in html:
                    logging.warning(f"Обнаружена CAPTCHA или ошибка 503 на {url}")
                    return None
                return html
            except Exception as e:
                logging.error(f"Ошибка загрузки {url}, попытка {attempt + 1}: {e}")
                if attempt == retries - 1:
                    return None
                sleep(uniform(2, 5))
        return None

    def urls_collect(self, search_query, max_articles_per_query, existing_urls):
        # Сбор ссылок на статьи по запросу
        urls = set()
        page = 1
        encoded_query = quote(search_query)
        max_pages = 50

        logging.info(f"Собираем ссылки для запроса: '{search_query}'")
        while len(urls) < max_articles_per_query and page <= max_pages:
            search_url = f"{self.base_url}/search?q={encoded_query}&page={page}"
            content = self.fetch_content(search_url)
            if not content:
                logging.warning(f"Не удалось загрузить страницу поиска для {search_query}")
                break

            soup = BeautifulSoup(content, 'lxml')
            links = soup.select('a[href^="/article"]')
            new_urls_found = False

            for link in links:
                href = link.get('href')
                full_url = self.base_url + href
                if (href.startswith('/article') and 
                    full_url not in urls and 
                    full_url not in existing_urls):
                    urls.add(full_url)
                    new_urls_found = True
                    if len(urls) >= max_articles_per_query:
                        break

            if not new_urls_found:
                break

            page += 1
            sleep(uniform(self.min_time_sleep, self.max_time_sleep))

        logging.info(f"Собрано {len(urls)} ссылок для '{search_query}'")
        return list(urls)

    def text_collect(self, url):
        # Сбор данных одной статьи по ссылке
        logging.info(f"Открываем статью: {url}")
        html = self.fetch_content(url)
        if not html:
            logging.warning(f"Не удалось загрузить статью: {url}")
            return None

        soup = BeautifulSoup(html, 'lxml')
        try:
            # Извлечение заголовка
            title = soup.find('h1') or soup.find('meta', attrs={'property': 'og:title'})
            title = title.text.strip() if hasattr(title, 'text') else title['content'] if title else "Не найдено"

            # Извлечение авторов
            authors = [meta['content'] for meta in soup.find_all('meta', attrs={'name': 'citation_author'})]
            authors_str = ', '.join(authors) if authors else "Не найдено"

            # Извлечение даты публикации
            pub_date = soup.find('meta', attrs={'name': 'citation_publication_date'})
            publication_date = pub_date['content'] if pub_date else "Не найдено"

            # Извлечение журнала
            journal = soup.find('meta', attrs={'name': 'citation_journal_title'})
            citation_journals = journal['content'] if journal else "Не найдено"

            # Извлечение ключевых слов
            keywords = soup.find('meta', attrs={'name': 'citation_keywords'})
            keywords = keywords['content'] if keywords else "Не найдено"

            # Извлечение аннотации
            abstract = soup.find('meta', attrs={'name': 'eprints.abstract'}) or soup.find('meta', attrs={'name': 'description'})
            anno = abstract['content'] if abstract else "Не найдено"

            # Извлечение текста статьи
            text_block = soup.find('div', class_='ocr') or soup.find('div', class_='content')
            if text_block:
                paragraphs = [p.get_text(strip=True) for p in text_block.find_all('p') 
                            if p.get_text(strip=True) and len(p.get_text(strip=True)) > 30]
                text = '\n\n'.join(paragraphs) if paragraphs else "Текст отсутствует"
            else:
                text = "Не найдено"

            # Проверка, что статья валидна
            if len(text) < 100 or title == "Не найдено":
                logging.warning(f"Статья {url} невалидна (короткий текст или нет заголовка)")
                return None

            logging.info(f"Успешно собраны данные статьи: {title}")
            return {
                'data': {
                    'title': title,
                    'authors': authors_str,
                    'publication_date': publication_date,
                    'citation_journals': citation_journals,
                    'keywords': keywords,
                    'anno': anno,
                    'url': url,
                    'text': text,
                    'fetch_date': datetime.now().strftime('%Y-%m-%d'),
                    'ncr_category': self.assign_ncr_category(url)
                }
            }
        except Exception as e:
            logging.error(f"Ошибка парсинга статьи {url}: {e}")
            return None

    def assign_ncr_category(self, url):
        # Определение категории НЦР для статьи
        for category, queries in goals_and_tasks.items():
            for query in queries:
                if any(word in url.lower() for word in query.split()):
                    return category
        return "Не определено"

def save_articles(articles, filename):
    # Сохранение собранных статей в JSON файл
    with open(filename, 'w', encoding='utf-8') as f:
        json.dump(articles, f, ensure_ascii=False, indent=4)
    logging.info(f"Сохранено {len(articles)} статей в {filename}")

def load_existing_articles(filename):
    # Загрузка существующих статей из файла
    if os.path.exists(filename):
        with open(filename, 'r', encoding='utf-8') as f:
            return json.load(f)
    return {}

def main():
    base_url = "https://cyberleninka.ru"
    collector = TextCollector(base_url=base_url, min_time_sleep=MIN_TIME_SLEEP, max_time_sleep=MAX_TIME_SLEEP)

    # Загружаем существующие статьи
    all_articles = load_existing_articles(OUTPUT_FILE)
    existing_urls = set(article['data']['url'] for article in all_articles.values())
    article_counter = len(all_articles)
    logging.info(f"Начинаем с {article_counter} существующих статей")

    remaining_articles = TOTAL_ARTICLES - article_counter
    if remaining_articles <= 0:
        logging.info("Цель уже достигнута!")
        collector.close()
        return

    # Собираем все запросы
    all_queries = [task for tasks in goals_and_tasks.values() for task in tasks]
    articles_per_query = max(remaining_articles // len(all_queries), 5)  # Минимум 5 статей на запрос

    try:
        # Шаг 1: Собираем все ссылки по запросам
        all_urls = []
        for query in all_queries:
            urls = collector.urls_collect(query, articles_per_query, existing_urls)
            all_urls.extend(urls)
            existing_urls.update(urls)  # Добавляем новые ссылки в исключения

        all_urls = all_urls[:remaining_articles]  # Ограничиваем количество
        logging.info(f"Всего собрано {len(all_urls)} уникальных ссылок для обработки")

        # Шаг 2: По одной открываем статьи и собираем данные
        with tqdm(total=len(all_urls), desc="Сбор статей") as pbar:
            for url in all_urls:
                article = collector.text_collect(url)
                if article:
                    article_counter += 1
                    all_articles[str(article_counter)] = article
                    save_articles(all_articles, OUTPUT_FILE)  # Сохраняем после каждой статьи
                    logging.info(f"Добавлена статья #{article_counter}: {article['data']['title']}")
                else:
                    logging.warning(f"Пропущена статья {url}")
                
                pbar.update(1)
                sleep(uniform(MIN_TIME_SLEEP, MAX_TIME_SLEEP))

        logging.info(f"Итог: собрано {len(all_articles)} статей")

    except Exception as e:
        logging.error(f"Ошибка в основном процессе: {e}")
        save_articles(all_articles, OUTPUT_FILE)
    finally:
        collector.close()

if __name__ == "__main__":
    main()