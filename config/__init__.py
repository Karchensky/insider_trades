"""
Configuration module for insider trades system.
"""

from .contract_selection import (
    ContractSelectionStrategy,
    get_active_strategy,
    get_strategy_config,
    DEFAULT_STRATEGY,
)

__all__ = [
    'ContractSelectionStrategy',
    'get_active_strategy',
    'get_strategy_config',
    'DEFAULT_STRATEGY',
]
