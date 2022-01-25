from ast import List
import asyncio
from graphlib import TopologicalSorter
from logging import Logger, getLogger
import time
from pipeline.errors import NotFoundNodeException, NotFoundScriptException
from typing import Dict, Sequence, Set, Type
from pipeline.node import Node
from pipeline.script import BaseScript

Graph = Dict[str, Sequence[str]]

def fetch_node_codes_from_graph(graph: Graph) -> Set[str]:
    """
    Fetch all node codes from the graph.
    """
    node_codes = set()
    for node_code, dependencies in graph.items():
        node_codes.add(node_code)
        for dependency in dependencies:
            node_codes.add(dependency)
    return node_codes


class Pipeline(object):
    graph: Graph  # node_code -> [next node_codes]
    nodes: List[Node]
    scripts: List[BaseScript]
    logger: Logger
    tp_sorter: TopologicalSorter

    def __init__(self, graph: Dict[str, List[str]], nodes: List[Node], scripts: List[BaseScript], logger: Logger = None):
        self.graph = graph
        self.nodes = nodes
        self.scripts = scripts
        if not logger:
            logger = getLogger(__name__)
        self.logger = logger

        self.node_map = {node.code: node for node in self.nodes}
        self.script_map = {script.code: script for script in self.scripts}
        self.node_script_map = {
            node.code: node.script_code for node in self.nodes}

        self.tp_sorter = TopologicalSorter(self.graph)

    def _next(self) -> Sequence[Type]:
        return self.tp_sorter.get_ready()
    
    def _has_next(self) -> bool:
        return self.tp_sorter.is_active()

    def _done(self, node_code_list: Sequence[str]):
        self.tp_sorter.done(*node_code_list)
    
    def check(self):
        node_codes = fetch_node_codes_from_graph(self.graph)
        for node_code in node_codes:
            if node_code not in self.node_map:
                raise NotFoundNodeException("Node not found, node code: {}".format(node_code))
            script_code = self.node_script_map.get(node_code)
            if not script_code:
                raise NotFoundScriptException("Script code not found, node code: {}".format(node_code))
            if script_code not in self.script_map:
                raise NotFoundScriptException("Script not found, node code: {}, script code: {}".format(node_code, script_code))
        self.tp_sorter.prepare()
    
    async def _execute_one(self, data: Dict, context: Dict, node_code: str, step: int):
        start_time = int(time.time() * 1000)
        node = self.node_map[node_code]
        script_code = node.script_code
        script = self.script_map[node_code]
        script_obj = script(data, context, self.logger)
        log_prefix = f"[_execute_one][step:{step}][node:{node_code}][script:{script_code}]"
        self.logger.info(f"{log_prefix} start execute")
        try:
            await script_obj.handle()
        except Exception as e:
            self.logger.exception(f"{log_prefix} execute failed, error: {e}")
        finally:
            end_time = int(time.time() * 1000)
            cost_time = end_time - start_time
            self.logger.info(f"{log_prefix} end execute, cost time: {cost_time}ms")
    
    async def execute(self, data: Dict, context: Dict, node_codes: List[str], step: int):
        log_prefix = f"[execute][step:{step}]"
        self.logger.info(f"{log_prefix} start execute, ready nodes is {','.join(node_codes)}")
        await asyncio.gather(*[self._execute_one(data, context, node_code, step) for node_code in node_codes])
        self.logger.info(f"{log_prefix} end execute")
    
    async def run(self, data: Dict, context: Dict={}):
        self.check()
        step = 1
        while self._has_next():
            ready_node_codes = self._next()
            await self.execute(data, context, ready_node_codes, step)
            self._done(ready_node_codes)
            step += 1