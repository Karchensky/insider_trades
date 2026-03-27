"""
Contract Selection Strategy Configuration

Defines available strategies for selecting the "best" contract from
candidates during anomaly detection.

Strategies:
- max_volume: Highest session volume (current default)
- max_gamma: Highest gamma value
- best_rr: Best risk/reward ratio (gamma * vega / |theta|)
- atm_preference: Closest to ATM with sufficient volume
- model_ranked: Highest predicted P(TP100) from ML model
"""

import os
from typing import Dict, Any
from enum import Enum


class ContractSelectionStrategy(Enum):
    """Available contract selection strategies."""
    MAX_VOLUME = 'max_volume'
    MAX_GAMMA = 'max_gamma'
    BEST_RR = 'best_rr'
    ATM_PREFERENCE = 'atm_preference'
    MODEL_RANKED = 'model_ranked'


DEFAULT_STRATEGY = ContractSelectionStrategy.MAX_VOLUME

STRATEGY_DESCRIPTIONS = {
    ContractSelectionStrategy.MAX_VOLUME: "Select contract with highest trading volume",
    ContractSelectionStrategy.MAX_GAMMA: "Select contract with highest gamma (price sensitivity)",
    ContractSelectionStrategy.BEST_RR: "Select contract with best risk/reward ratio (gamma*vega/|theta|)",
    ContractSelectionStrategy.ATM_PREFERENCE: "Select contract closest to at-the-money with sufficient volume",
    ContractSelectionStrategy.MODEL_RANKED: "Select contract with highest ML-predicted P(TP100)",
}


def get_active_strategy() -> ContractSelectionStrategy:
    """
    Get the currently active contract selection strategy.
    
    Can be overridden via environment variable CONTRACT_SELECTION_STRATEGY.
    
    Returns:
        ContractSelectionStrategy enum value
    """
    strategy_name = os.getenv('CONTRACT_SELECTION_STRATEGY', DEFAULT_STRATEGY.value)
    
    try:
        return ContractSelectionStrategy(strategy_name.lower())
    except ValueError:
        return DEFAULT_STRATEGY


def get_strategy_config() -> Dict[str, Any]:
    """
    Get full configuration for contract selection.
    
    Returns:
        Dict with strategy settings
    """
    return {
        'active_strategy': get_active_strategy().value,
        'available_strategies': [s.value for s in ContractSelectionStrategy],
        'descriptions': {s.value: d for s, d in STRATEGY_DESCRIPTIONS.items()},
        'min_volume_threshold': int(os.getenv('CONTRACT_MIN_VOLUME', '50')),
        'price_range_min': float(os.getenv('CONTRACT_PRICE_MIN', '0.05')),
        'price_range_max': float(os.getenv('CONTRACT_PRICE_MAX', '5.00')),
        'atm_moneyness_tolerance': float(os.getenv('ATM_MONEYNESS_TOLERANCE', '0.05')),
    }
