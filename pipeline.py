#!/usr/bin/python3
# -*- coding: utf-8 -*-

"""
    Module documentation goes here
    @Author: Daryl
    @Date: 2022-01-09 13:53:50
"""
import asyncio
import logging
from graphlib import TopologicalSorter
from typing import Dict, List

from constants import TaskFunc


class Pipeline:
    def __init__(self, graph: Dict[TaskFunc, List[TaskFunc]], logger: logging.Logger = None):
        """
            Parameters:
                graph: A dictionary of task functions and their dependencies.
        """
        self.graph = graph
        self.sorter = TopologicalSorter(self.graph)
        if logger is None:
            logger = logging.getLogger(__name__)
        self.logger = logger

    async def run(self, task_data: Dict, context: Dict):
        """
            Parameters:
                task_data: task data to be passed to the task functions.
                context: context data to be passed to the task functions.
        """
        self.sorter.prepare()
        while self.sorter.is_active():
            ready_list = self.sorter.get_ready()
            await asyncio.gather(*[func(task_data, context) for func in ready_list])
            self.sorter.done(*ready_list)
