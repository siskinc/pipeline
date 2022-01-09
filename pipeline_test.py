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
