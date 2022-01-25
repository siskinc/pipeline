#!/usr/bin/python3
# -*- coding: utf-8 -*-

"""
    Dags Node structure
    @Author: Daryl
    @Date: 2022-01-25 09:27:58
"""
from attrs import define

@define
class Node(object):
    code: str # Node code
    name: str # Node name
    script_code: str # script code
