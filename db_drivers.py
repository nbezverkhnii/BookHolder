"""
Драйвера для работы с базой данных.
Реализован контекстный менеджер по правилам PEP249
"""
from typing import Any
from abc import ABC, abstractmethod
import sqlite3


class DBContextManager(ABC):
    """
    Класс, описывающий семейство контекстных менеджеров
    ...
    """

    @abstractmethod
    def __init__(self, db_driver, config: dict) -> None:
        """
        Конструктор класса

        :parameter: db_driver
            ...
        :parameter: config
            ...

        """
        self.__db_driver = db_driver
        self.config = config

    def __enter__(self) -> Any:
        """
        Выполняет подключение к базе данных по PEP 249
        ...
        """
        self.connection = self.__db_driver.connect(**self.config)
        self.coursor = self.connection.cursor()
        return self.coursor

    def __exit__(self, exc_type, exc_value, traceback):
        """
        Выход из контекстного менеджера
        ...
        """

        self.coursor.close()
        if exc_type is None:
            self.connection.commit()
        else:
            self.connection.rollback()

        self.connection.close()


class SQLiteDriver(DBContextManager):
    """
    Драйвер для sqlite3
    """
    DB_NAME = 'book_db.sqlite3'
    DB_DRIVER = sqlite3

    def __init__(self, database=DB_NAME):
        config = {'database': database}
        super().__init__(self.DB_DRIVER, config)


# class MySQLDriver(DBContextManager):
#     """
#     Драйвер для MySQL
#     """
#     ...`
    # DB_DRIVER = mysql.connector
    #
    # def __init__(self, user: str, password: str, host: str, database: str):
    #     config = {'user': user, 'password': password, 'host': host, 'database': database}
    #     super().__init__(self.DB_DRIVER, config)
