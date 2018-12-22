#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# datetime:2018/12/18 11:34
from setuptools import setup
setup(
    name='click_demo1',
    version='0.1',
    py_modules=['click_demo1'],
    install_requiers=[
        'Click',
    ],
    entry_points="""
            [console_scripts]
            click_demo1=click_demo1:cli
    
    """
)
