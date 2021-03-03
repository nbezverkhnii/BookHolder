"""
Это финальное задание к курсу PY200
Реализован класс BookHolder для хранения книг в базе данных sqlite3
"""
import csv
from typing import Optional, Iterator
import random
from faker import Faker
from db_drivers import SQLiteDriver


class BookHolder:
    DATABASE = 'library.sqlite3'
    BOOKS_TABLE = 'books'

    def __init__(self) -> None:
        self.__driver = SQLiteDriver(self.DATABASE)
        self.create_books_table()

    def create_books_table(self):
        """
        Создает таблицу в базе данных sqlite3
        """
        sql = f"""
        CREATE TABLE IF NOT EXISTS {self.BOOKS_TABLE} (
            id INTEGER PRIMARY KEY AUTOINCREMENT,  -- первичный ключ, идентифицирует записи в таблице БД
            title TEXT NOT NULL, -- название книг не может быть пустым
            authors TEXT NOT NULL, -- авторы книги, не может быть пустым
            isbn13 TEXT UNIQUE NOT NULL, -- идентификатор книги не может повторяться
            pages INTEGER NOT NULL, -- количество страниц в книге не может быть пустым
            year INTEGER NOT NULL, -- год издания книги не может быть пустым
            price REAL NOT NULL, -- цена книги не может быть пустой
            discount INTEGER -- а вот скидка может быть пустой
        ); 
        """
        with self.__driver as cursor:
            cursor.execute(sql)

    def insert_book(self, title: str, authors: str, isbn13: str, pages: int, year: int, price: float,
                    discount: Optional[int] = None) -> None:
        """
        Осуществляет вставку значений в таблицу

        :param title:
            Название книги
        :param authors:
            Авторы книги
        :param isbn13:
            isbn книги
        :param pages:
            Количество страниц в книге
        :param year:
            Год книги
        :param price:
            Цена книги
        :param discount:
            Скидка на книгу
        :return:
            None
        """
        sql = f"""
        INSERT INTO {self.BOOKS_TABLE}(
            title,
            authors,
            isbn13,
            pages,
            year,
            price,
            discount
        )
        VALUES (?, ?, ?, ?, ?, ?, ?);  -- оставляем заглушки в виде ?
        """
        with self.__driver as cursor:
            cursor.execute(sql, [title, authors, isbn13, pages, year, price, discount])

    @staticmethod
    def book_generator(count: int = 10) -> Iterator[tuple]:
        """

        (
            title: str,
            authors: str,
            isbn13: str,
            pages: int,
            year: int,
            price: float,
            discount: Optional[int] = None
        )

        :param count: количество книг для случайной генерации
        :return: Кортеж с описанием книги
        """
        fake = Faker(locale='ru_RU')

        def fake_authors_gen() -> str:
            """
            Генерирует строку с именами авторов книги (от 1 до 3 авторов)

            :return:
                Авторы книги
            """
            author = [fake.first_name() + ' ' + fake.last_name() for _ in range(random.randint(1, 3))]
            return ', '.join(author)

        for _ in range(count):
            fake_title = fake.text(max_nb_chars=20)
            fake_authors = fake_authors_gen()
            fake_isbn13 = fake.isbn13()
            fake_page = fake.pyint(min_value=0, max_value=1000)
            fake_year = fake.pyint(min_value=1900, max_value=2021)
            fake_price = fake.pyfloat(min_value=0, max_value=10000, right_digits=2)
            fake_discount = fake.random_element([None, *range(10, 91, 10)])

            yield fake_title, fake_authors, fake_isbn13, fake_page, fake_year, fake_price, fake_discount

    def init_books(self, count):
        """
        ...
        """
        sql = f"""
        INSERT INTO {self.BOOKS_TABLE}(
            title,
            authors,
            isbn13,
            pages,
            year,
            price,
            discount
        )
        VALUES (?, ?, ?, ?, ?, ?, ? ); 
        """

        with self.__driver as cursor:
            cursor.executemany(sql, self.book_generator(count))

    def get_book_by_year(self, year: int) -> Iterator:
        """
        Возвращает все книги из таблицы с указаным в параметре year годом

        :param year:
            Год издания книги в базе данных
        :return:
            Итератор книг из баз данных
        """
        if not isinstance(year, int):
            raise TypeError('year должно быть int')

        sql = f"""
        SELECT *
        FROM {self.BOOKS_TABLE}
        WHERE year = {year};
        """

        with self.__driver as cursor:
            for row in cursor.execute(sql):
                yield row

    def get_book_by_price_less(self, price: float) -> list:
        """
        Возвращает список книг с ценой ниже заданной в параметре price

        :param price:
        :return:
            Списко книг
        """
        if not isinstance(price, (float, int)):
            raise TypeError('price должно быть float')

        sql = f"""
        SELECT *
        FROM {self.BOOKS_TABLE}
        WHERE price < {price}
        ORDER BY year;
        """

        with self.__driver as cursor:
            return cursor.execute(sql).fetchall()

    def get_book_by_page_greater(self, pages: float) -> list:
        """
        Возвращает книги в которых страниц больше, чем в параметре pages

        :param pages:
            Количество страниц в книге
        :return:
            Список книг
        """
        if not isinstance(pages, (float, int)):
            raise TypeError('pages должно быть float')

        sql = f"""
        SELECT *
        FROM {self.BOOKS_TABLE}
        WHERE pages > {pages}
        ORDER BY year;
        """

        with self.__driver as cursor:
            return cursor.execute(sql).fetchall()

    def get_number_of_books(self) -> int:
        """
        Возвращает количество книг в библиотеке
        """
        sql = f"""
        SELECT COUNT(*)
        FROM {self.BOOKS_TABLE};
        """

        with self.__driver as cursor:
            return cursor.execute(sql).fetchone()[0]

    def get_number_of_books_by_year(self, year: int) -> int:
        """
        Возвращает количество книг, которые изданы в определенной год

        :param year:
            Год издания книги
        :return:
            Количество книг, который изданы в заданный год
        """
        if not isinstance(year, int):
            raise TypeError('year должно быть int')

        sql = f"""
        SELECT COUNT(*)
        FROM {self.BOOKS_TABLE}
        WHERE year = {year};
        """

        with self.__driver as cursor:
            return cursor.execute(sql).fetchone()[0]

    def export_books_to_csv(self, limit: int) -> None:
        """
        Экспортирует бузу данных в .csv файл.
        В файл пишет ограниченное число строк, которое определяет аргуемнт limit

        :param limit:
            Сколько строк из базы данных запишется в файл
        :return:
            None
        """
        if not isinstance(limit, int):
            raise TypeError('limit должно быть int')

        sql = f"""
        SELECT *
        FROM {self.BOOKS_TABLE}
        LIMIT {limit};
        """

        with self.__driver as cursor:
            with open('output.csv', 'w') as file:
                data = cursor.execute(sql)
                writer = csv.writer(file)
                writer.writerow(['title', 'authors', 'isbn13', 'pages', 'year', 'price', 'discount'])
                writer.writerows(data)

    def delete_book(self, id_: int) -> None:
        """
        Удаляет из базы данных книгу с указанным id

        :param id_:
            id киниг, которую нужно удалить
        :return:
            None
        """
        if not isinstance(id_, int):
            raise TypeError('id_ должно быть int')

        sql = f"""
        DELETE 
        FROM {self.BOOKS_TABLE}
        WHERE id = {id_};
        """

        with self.__driver as cursor:
            cursor.execute(sql)

    def clean_book_table(self) -> None:
        """
        Чистит таблицу с данными

        :return:
            None
        """
        sql = f"DELETE FROM {self.BOOKS_TABLE};"

        with self.__driver as cursor:
            cursor.execute(sql)

    def delete_book_table(self) -> None:
        """
        Удаляем таблицу с данными

        :return:
            None
        """
        sql = f"DROP table {self.BOOKS_TABLE};"

        with self.__driver as cursor:
            cursor.execute(sql)


if __name__ == '__main__':
    library = BookHolder()
    library.init_books(1000)

    for book in library.get_book_by_page_greater(900):
        print(book)

    #print(library.get_book_by_price_less(1000))

    print(library.get_number_of_books())


    #library.delete_book_table()


