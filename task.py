from errors import CannotFoundTask
from constants import TaskFunc, TaskFuncName
from types import FunctionType, CodeType

class TaskFactory(object):
    
    @classmethod
    def create_task_func(cls, code_str: str) -> TaskFunc:
        """
        Creates a task function from a string.
        """
        result = compile(code_str, '<string>', 'exec')
        co_consts = result.co_consts[:-1]
        functions = dict(zip(co_consts[1::2], co_consts[::2]))
        if TaskFuncName not in functions:
            raise CannotFoundTask(f'Cannot found {TaskFuncName} in {code_str}')
        task_code = functions[TaskFuncName]
        if not isinstance(task_code, CodeType):
            raise CannotFoundTask(f'{TaskFuncName} is not a code object')
        return FunctionType(functions[TaskFuncName], {'__builtins__':globals()['__builtins__']})
