import copy
import inspect


class QueryExpression:
    def __init__(self, column, equality, value, value_type="value", keyword=None):
        self.column = column
        self.equality = equality
        self.value = value
        self.value_type = value_type
        self.keyword = keyword

class UpdateQueryExpression:
    def __init__(self, column, value=None, update_type='keyvalue'):
        self.column = column
        self.value = value
        self.update_type = update_type


class SubSelectExpression:
    def __init__(self, builder):
        self.builder = builder

class SubGroupExpression:
    def __init__(self, builder):
        self.builder = builder


class QueryBuilder:

    _action = "select"

    def __init__(self, grammar, connection=None, table="", connection_details={}):
        """QueryBuilder initializer

        Arguments:
            grammar {masonite.orm.grammar.Grammar} -- A grammar class.

        Keyword Arguments:
            connection {masonite.orm.connection.Connection} -- A connection class (default: {None})
            table {str} -- the name of the table (default: {""})
        """
        self.grammar = grammar
        self.table = table
        self.connection = connection
        self.connection_details = connection_details
        self._scopes = {}
        self.boot()
        self.set_action("select")

    def set_scope(self, cls, name):
        self._scopes.update({name: cls})

        return self

    def __getattr__(self, attribute):
        if attribute in self._scopes:
            # print('calling', attribute, 'on', cls)
            return getattr(self._scopes[attribute], attribute)
            # return cls

        raise AttributeError(
            "'QueryBuilder' object has no attribute '{}'".format(attribute)
        )

    def boot(self):
        self._columns = "*"

        self._sql = ""
        self._sql_binding = ""
        self._bindings = ()

        self._updates = {}

        self._wheres = ()
        self._order_by = ()
        self._group_by = ()

        self._aggregates = ()

        self._limit = False
        self._action = None

    def select(self, *args):
        self._columns = list(args)
        return self

    def create(self, creates):
        self._columns = creates
        self._action = "insert"
        return self

    def delete(self, column=None, value=None):
        if column and value:
            self.where(column, value)
        self._action = "delete"
        return self

    def where(self, column, value=None):
        if inspect.isfunction(column):
            builder = column(self.new())
            self._wheres += (
                (
                    QueryExpression(
                        None, "=", SubGroupExpression(builder)
                    )
                ),
            )
        elif isinstance(value, QueryBuilder):
            self._wheres += (
                (QueryExpression(column, "=", SubSelectExpression(value))),
            )
        else:
            self._wheres += ((QueryExpression(column, "=", value, "value")),)
        # self._wheres += ((column, "=", value),)
        return self

    def or_where(self, column, value):
        if isinstance(value, QueryBuilder):
            self._wheres += (
                (QueryExpression(column, "=", SubSelectExpression(value))),
            )
        else:
            self._wheres += (
                (QueryExpression(column, "=", value, "value", keyword="or")),
            )
        # self._wheres += ((column, "=", value),)
        return self

    def where_exists(self, value):
        if isinstance(value, QueryBuilder):
            self._wheres += (
                (QueryExpression(None, "EXISTS", SubSelectExpression(value))),
            )
        else:
            self._wheres += ((QueryExpression(None, "EXISTS", value, "value")),)
        # self._wheres += ((column, "=", value),)
        return self

    def where_null(self, column):
        self.where(column, None)
        return self

    def where_not_null(self, column):
        self.where(column, True)
        return self

    def where_in(self, column, wheres=[]):
        if isinstance(wheres, QueryBuilder):
            self._wheres += (
                (QueryExpression(column, "IN", SubSelectExpression(wheres))),
            )
        else:
            wheres = [str(x) for x in wheres]
            self._wheres += ((QueryExpression(column, "IN", wheres)),)
        return self

    def where_column(self, column1, column2):
        self._wheres += ((QueryExpression(column1, "=", column2, "column")),)
        return self

    def limit(self, amount):
        self._limit = amount
        return self

    def update(self, updates):
        self._updates = (UpdateQueryExpression(updates),)
        self._action = "update"
        return self

    def increment(self, column, value=1):
        self._updates = (UpdateQueryExpression(column, value, update_type='increment'),)
        self._action = "update"
        return self

    def decrement(self, column, value=1):
        self._updates = (UpdateQueryExpression(column, value, update_type='decrement'),)
        self._action = "update"
        return self

    def sum(self, column):
        self.aggregate("SUM", "{column}".format(column=column))
        return self

    def count(self, column="*"):
        self.aggregate("COUNT", "{column}".format(column=column))
        return self

    def max(self, column):
        self.aggregate("MAX", "{column}".format(column=column))
        return self

    def order_by(self, column, direction="ASC"):
        self._order_by += ((column, direction),)
        return self

    def group_by(self, column):
        self._group_by += (column,)
        return self

    def aggregate(self, aggregate, column):
        self._aggregates += ((aggregate, column),)

    def first(self):
        self._action = "select"
        return (
            self.connection()
            .make_connection()
            .query(self.limit(1).to_qmark(), self._bindings, results=1)
        )

    def all(self):
        return (
            self.connection().make_connection().query(self.to_qmark(), self._bindings)
        )

    def get(self):
        self._action = "select"
        return (
            self.connection().make_connection().query(self.to_qmark(), self._bindings)
        )

    def set_action(self, action):
        self._action = action
        return self

    def get_grammar(self):
        return self.grammar(
            columns=self._columns,
            table=self.table,
            wheres=self._wheres,
            limit=self._limit,
            updates=self._updates,
            aggregates=self._aggregates,
            order_by=self._order_by,
            group_by=self._group_by,
            connection_details=self.connection.connection_details
            if self.connection
            else self.connection_details,
        )

    def to_sql(self):
        grammar = self.get_grammar()

        if not self._action:
            self.set_action("select")

        sql = getattr(
            grammar, "_compile_{action}".format(action=self._action)
        )().to_sql()
        self.boot()
        return sql

    def to_qmark(self):
        grammar = self.grammar(
            columns=self._columns,
            table=self.table,
            wheres=self._wheres,
            limit=self._limit,
            updates=self._updates,
            aggregates=self._aggregates,
            order_by=self._order_by,
            group_by=self._group_by,
            connection_details=self.connection.connection_details
            if self.connection
            else self.connection_details,
        )

        grammar = getattr(grammar, "_compile_{action}".format(action=self._action))(
            qmark=True
        )
        self.boot()
        self._bindings = grammar._bindings
        return grammar.to_qmark()

    def new(self):
        builder = QueryBuilder(
            grammar=self.grammar, connection=self.connection, table=self.table
        )

        return builder
