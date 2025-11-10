#!/bin/bash

# -*- coding: utf-8 -*-
# @Time    : 2021/10/15
# @Author  : github.com/Agrover112

echo -n "Running tests...."
echo  -e "\n Testing DataBase"
python3 test_db_connection.py
#echo -e "\nTesting xyz......" For future reference
#python3 test_xyz.py
echo "All tests done!"
echo -n -e "----------------------------------------------------------------------\n"
