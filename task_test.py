from task import TaskFactory
import unittest

class TestTaskFactory(unittest.IsolatedAsyncioTestCase):

    async def test_create_task(self):
        code_str = """
def task_func():
    return 'task_func'
def a():
    return 'a'
def handle():
    print('handle')
"""
        task_func = TaskFactory.create_task_func(code_str)
        task_func()