class ConnectionResolver:

    _connection_details = {}
    _connections = {}

    def __init__(self):
        from ..connections import (
            SQLiteConnection,
            PostgresConnection,
            MySQLConnection,
            MSSQLConnection,
        )

        from ..connections import ConnectionFactory

        self.connection_factory = ConnectionFactory()

        print('init, register')

        self.register(SQLiteConnection)
        self.register(PostgresConnection)
        self.register(MySQLConnection)
        self.register(MSSQLConnection)



    def set_connection_details(self, connection_details):
        self.__class__._connection_details = connection_details
        return self

    def get_connection_details(self):
        return self._connection_details

    def get_global_connections(self):
        return self._connections

    def remove_global_connection(self, name=None):
        self._connections.pop(name)

    def register(self, connection):
        self.connection_factory.register(connection.name, connection)

    def begin_transaction(self, name=None):

        if name is None:
            name = self.get_connection_details()["default"]

        connection = (
            self.connection_factory
            .make(name)(**self.get_connection_information(name))
            .make_connection()
            .begin()
        )
        self.__class__._connections.update({name: connection})

        return connection

    def commit(self, name=None):
        if name is None:
            name = self.get_connection_details()["default"]
        connection = self.get_global_connections()[name]
        self.remove_global_connection(name)
        connection.commit()

    def rollback(self, name=None):
        if name is None:
            name = self.get_connection_details()["default"]

        connection = self.get_global_connections()[name]
        self.remove_global_connection(name)
        connection.rollback()

    def get_connection_information(self, name):
        details = self.get_connection_details()
        return {
            "host": details.get(name, {}).get("host"),
            "database": details.get(name, {}).get("database"),
            "user": details.get(name, {}).get("user"),
            "port": details.get(name, {}).get("port"),
            "password": details.get(name, {}).get("password"),
            "prefix": details.get(name, {}).get("prefix"),
            "options": details.get(name, {}).get("options", {}),
            "full_details": details.get(name, {}),
        }
