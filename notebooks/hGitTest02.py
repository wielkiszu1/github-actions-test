# Databricks notebook source
spark.sql(""" insert into hGitPush select current_timestamp() as CurrTime """)
