# Databricks notebook source

# COMMAND ----------
from src.utils.logger import get_logger

# COMMAND ----------
logger = get_logger("load_click_data")
logger.info("Starting click data load")
