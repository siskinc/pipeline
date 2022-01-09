from task import TaskFactory
import unittest

from pipeline import Pipeline


class TestPipeline(unittest.IsolatedAsyncioTestCase):

    async def test_run(self):
        async def func_a(task_data, context):
            print("func_a")
            return context

        async def func_b(task_data, context):
            print("func_b")
            return context

        async def func_c(task_data, context):
            print("func_c")
            return context

        graph = {
            func_a: [func_b],
            func_b: [],
            func_c: [],
        }
        p = Pipeline(graph)
        await p.run({}, {})

    async def test_run_with_task(self):
        handle1_code_str = """
async def handle(task_data, context):
    print("handle1")
"""
        handle2_code_str = """
async def handle(task_data, context):
    print("handle2")
"""
        handle3_code_str = """
async def handle(task_data, context):
    print("handle3")
"""
        handle1_func = TaskFactory.create_task_func(handle1_code_str)
        handle2_func = TaskFactory.create_task_func(handle2_code_str)
        handle3_func = TaskFactory.create_task_func(handle3_code_str)
        graph = {
            handle1_func: [handle2_func],
            handle2_func: [],
            handle3_func: [],
        }
        p = Pipeline(graph)
        await p.run({}, {})