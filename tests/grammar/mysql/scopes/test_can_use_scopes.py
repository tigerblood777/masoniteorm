import unittest
from app.User import User
import inspect

from src.masonite.orm.models.Model import Model
from src.masonite.orm.mixins.scope import scope, global_scope

class MockUser(Model):

    @scope
    def active(query, status):
        return query.where('active', status)

    @scope
    def gender(query, status):
        return query.where('gender', status)


class TestMySQLScopes(unittest.TestCase):

    def test_can_get_sql(self):
        sql = "SELECT * FROM `users` WHERE `name` = 'joe'"
        self.assertEqual(sql, MockUser.where('name', 'joe').to_sql())
    
    def test_active_scope(self):
        sql = "SELECT * FROM `users` WHERE `active` = '1' AND `name` = 'joe'"
        self.assertEqual(sql, MockUser.active(1).where('name', 'joe').to_sql())
    
    def test_active_scope_with_params(self):
        sql = "SELECT * FROM `users` WHERE `active` = '2' AND `name` = 'joe'"
        self.assertEqual(sql, MockUser.active(2).where('name', 'joe').to_sql())
    
    def test_can_chain_scopes(self):
        sql = "SELECT * FROM `users` WHERE `active` = '2' AND `gender` = 'W' AND `name` = 'joe'"
        self.assertEqual(sql, MockUser.active(2).gender('W').where('name', 'joe').to_sql())
    
    # def test_can_use_global_scopes(self):
    #     sql = "SELECT * FROM `users` WHERE `deleted_at` IS NOT NULL AND `name` = 'joe'"
    #     print(MockUser.builder)
    #     self.assertEqual(sql, MockUser.where('name', 'joe').to_sql())


    