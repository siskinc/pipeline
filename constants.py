from typing import Awaitable, Callable, Dict

TaskFunc = Callable[[Dict, Dict], Awaitable[Dict]] # def func(task_data: Dict, task_context: Dict) -> Dict
TaskFuncName = "handle"