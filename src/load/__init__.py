"""
Apple Analytics ETL - Load Module
Data loading and Athena table management
"""

from .athena_table_manager_production import AthenaTableManager

__all__ = ['AthenaTableManager']
