#!/usr/bin/env python3
"""
TP100 Probability Model

Machine learning model to predict P(TP100) - the probability that an options
contract will hit +100% take profit before expiration.

Features:
- Walk-forward validation with 60-day train / 30-day test windows
- Logistic regression baseline with optional LightGBM upgrade
- Feature importance analysis
- Model persistence and versioning
- Integration with InsiderAnomalyDetector for real-time inference
"""

import os
import sys
import json
import pickle
import logging
from datetime import datetime, timedelta, date
from typing import Dict, List, Any, Optional, Tuple
from pathlib import Path

import numpy as np
import pandas as pd
from sklearn.linear_model import LogisticRegression
from sklearn.preprocessing import StandardScaler
from sklearn.metrics import (
    accuracy_score, precision_score, recall_score, f1_score,
    roc_auc_score, confusion_matrix, classification_report
)
from sklearn.model_selection import TimeSeriesSplit

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from database.core.connection import db

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

MODEL_DIR = Path(__file__).parent.parent / 'models'
MODEL_DIR.mkdir(exist_ok=True)


class TP100Model:
    """
    Machine learning model for predicting P(TP100).
    
    Uses historical anomaly data with labeled outcomes to train a classifier
    that predicts the probability of hitting +100% take profit.
    """
    
    FEATURE_COLUMNS = [
        'total_score',
        'high_conviction_score',
        'z_score',
        'otm_score',
        'volume_score',
        'volume_oi_ratio_score',
        'directional_score',
        'time_score',
        'greeks_theta_value',
        'greeks_gamma_value',
        'greeks_vega_value',
        'greeks_theta_percentile',
        'greeks_gamma_percentile',
        'greeks_vega_percentile',
        'greeks_otm_percentile',
        'volume_rank_percentile',
        'historical_tp100_rate',
        'historical_signal_count',
        'moneyness',
        'days_to_expiry',
        'iv_percentile',
        'gamma_theta_ratio',
        'total_magnitude',
        'call_put_ratio',
        'otm_call_percentage',
        'short_term_percentage',
    ]
    
    BOOLEAN_FEATURES = [
        'greeks_theta_met',
        'greeks_gamma_met',
        'greeks_vega_met',
        'greeks_otm_met',
        'is_high_conviction',
    ]
    
    def __init__(self, model_type: str = 'logistic'):
        """
        Initialize the TP100 model.
        
        Args:
            model_type: 'logistic' for LogisticRegression, 'lightgbm' for LightGBM
        """
        self.model_type = model_type
        self.model = None
        self.scaler = StandardScaler()
        self.feature_columns = self.FEATURE_COLUMNS + self.BOOLEAN_FEATURES
        self.model_version = None
        self.training_metrics = {}
        
    def _build_training_dataset(self, 
                                 min_date: Optional[date] = None,
                                 max_date: Optional[date] = None,
                                 min_magnitude: float = 20000) -> pd.DataFrame:
        """
        Build labeled training dataset from historical data.
        
        Args:
            min_date: Earliest date to include
            max_date: Latest date to include
            min_magnitude: Minimum magnitude threshold
        
        Returns:
            DataFrame with features and hit_tp_100 label
        """
        logger.info("Building training dataset...")
        
        conn = db.connect()
        try:
            query = """
            WITH filtered_anomalies AS (
                SELECT 
                    a.event_date,
                    a.symbol,
                    a.total_score,
                    a.high_conviction_score,
                    a.is_high_conviction,
                    a.z_score,
                    a.otm_score,
                    a.volume_score,
                    a.volume_oi_ratio_score,
                    a.directional_score,
                    a.time_score,
                    a.greeks_theta_value,
                    a.greeks_gamma_value,
                    a.greeks_vega_value,
                    a.greeks_theta_percentile,
                    a.greeks_gamma_percentile,
                    a.greeks_vega_percentile,
                    a.greeks_otm_percentile,
                    a.greeks_theta_met,
                    a.greeks_gamma_met,
                    a.greeks_vega_met,
                    a.greeks_otm_met,
                    a.volume_rank_percentile,
                    a.historical_tp100_rate,
                    a.historical_signal_count,
                    a.moneyness,
                    a.days_to_expiry,
                    a.iv_percentile,
                    a.gamma_theta_ratio,
                    a.total_magnitude,
                    a.call_put_ratio,
                    a.otm_call_percentage,
                    a.short_term_percentage,
                    a.recommended_option
                FROM daily_anomaly_snapshot a
                WHERE a.total_magnitude >= %s
                  AND COALESCE(a.is_bot_driven, FALSE) = FALSE
                  AND COALESCE(a.is_earnings_related, FALSE) = FALSE
                  AND a.recommended_option IS NOT NULL
            """
            
            params = [min_magnitude]
            
            if min_date:
                query += " AND a.event_date >= %s"
                params.append(min_date)
            if max_date:
                query += " AND a.event_date <= %s"
                params.append(max_date)
            
            query += """
            ),
            with_outcomes AS (
                SELECT 
                    fa.*,
                    o_entry.close_price AS entry_price,
                    oc.expiration_date,
                    (
                        SELECT MAX(o_future.close_price)
                        FROM daily_option_snapshot o_future
                        WHERE o_future.contract_ticker = fa.recommended_option
                          AND o_future.date > fa.event_date
                          AND o_future.date <= oc.expiration_date
                    ) AS max_future_price
                FROM filtered_anomalies fa
                INNER JOIN daily_option_snapshot o_entry 
                    ON fa.recommended_option = o_entry.contract_ticker 
                    AND fa.event_date = o_entry.date
                INNER JOIN option_contracts oc 
                    ON fa.recommended_option = oc.contract_ticker
                WHERE o_entry.close_price BETWEEN 0.05 AND 5.00
                  AND o_entry.volume > 50
            )
            SELECT 
                *,
                CASE 
                    WHEN max_future_price >= 2.0 * entry_price THEN 1 
                    ELSE 0 
                END AS hit_tp_100
            FROM with_outcomes
            WHERE max_future_price IS NOT NULL
            ORDER BY event_date
            """
            
            df = pd.read_sql(query, conn, params=params)
            logger.info(f"Built dataset with {len(df)} records")
            
            if len(df) > 0:
                tp100_rate = df['hit_tp_100'].mean() * 100
                logger.info(f"Overall TP100 rate: {tp100_rate:.1f}%")
            
            return df
            
        finally:
            conn.close()
    
    def _prepare_features(self, df: pd.DataFrame) -> Tuple[np.ndarray, np.ndarray]:
        """
        Prepare feature matrix and target vector.
        
        Args:
            df: DataFrame with raw features
        
        Returns:
            Tuple of (X features array, y target array)
        """
        available_features = [c for c in self.feature_columns if c in df.columns]
        
        X = df[available_features].copy()
        
        for col in X.columns:
            if X[col].dtype == 'bool':
                X[col] = X[col].astype(int)
            elif X[col].dtype == 'object':
                X[col] = pd.to_numeric(X[col], errors='coerce')
        
        X = X.fillna(0)
        
        y = df['hit_tp_100'].values
        
        return X.values, y, available_features
    
    def train(self, 
              train_days: int = 60,
              test_days: int = 30,
              n_splits: int = 5) -> Dict[str, Any]:
        """
        Train the model using walk-forward validation.
        
        Args:
            train_days: Number of days in each training window
            test_days: Number of days in each test window
            n_splits: Number of walk-forward splits
        
        Returns:
            Dict with training metrics and results
        """
        logger.info(f"Training TP100 model with {n_splits} walk-forward splits...")
        
        df = self._build_training_dataset()
        
        if len(df) < 100:
            logger.warning(f"Insufficient data for training: {len(df)} records")
            return {'success': False, 'error': 'Insufficient training data'}
        
        X, y, feature_names = self._prepare_features(df)
        dates = df['event_date'].values
        
        all_metrics = []
        all_predictions = []
        
        unique_dates = np.unique(dates)
        n_dates = len(unique_dates)
        
        if n_dates < train_days + test_days:
            logger.warning(f"Not enough date range for walk-forward: {n_dates} days")
            X_scaled = self.scaler.fit_transform(X)
            self.model = self._create_model()
            self.model.fit(X_scaled, y)
            
            y_pred = self.model.predict(X_scaled)
            y_prob = self.model.predict_proba(X_scaled)[:, 1]
            
            metrics = self._calculate_metrics(y, y_pred, y_prob)
            self.training_metrics = metrics
            self.model_version = datetime.now().strftime('%Y%m%d_%H%M%S')
            
            return {
                'success': True,
                'model_version': self.model_version,
                'metrics': metrics,
                'feature_importance': self._get_feature_importance(feature_names),
                'training_samples': len(y),
                'walk_forward_splits': 0
            }
        
        for i in range(n_splits):
            test_end_idx = n_dates - i * test_days
            test_start_idx = test_end_idx - test_days
            train_end_idx = test_start_idx
            train_start_idx = max(0, train_end_idx - train_days)
            
            if train_start_idx >= train_end_idx or test_start_idx >= test_end_idx:
                continue
            
            train_dates = unique_dates[train_start_idx:train_end_idx]
            test_dates = unique_dates[test_start_idx:test_end_idx]
            
            train_mask = np.isin(dates, train_dates)
            test_mask = np.isin(dates, test_dates)
            
            X_train, y_train = X[train_mask], y[train_mask]
            X_test, y_test = X[test_mask], y[test_mask]
            
            if len(y_train) < 20 or len(y_test) < 10:
                continue
            
            scaler = StandardScaler()
            X_train_scaled = scaler.fit_transform(X_train)
            X_test_scaled = scaler.transform(X_test)
            
            model = self._create_model()
            model.fit(X_train_scaled, y_train)
            
            y_pred = model.predict(X_test_scaled)
            y_prob = model.predict_proba(X_test_scaled)[:, 1]
            
            metrics = self._calculate_metrics(y_test, y_pred, y_prob)
            metrics['split'] = i
            metrics['train_samples'] = len(y_train)
            metrics['test_samples'] = len(y_test)
            all_metrics.append(metrics)
            
            all_predictions.extend(zip(y_test, y_prob))
        
        X_scaled = self.scaler.fit_transform(X)
        self.model = self._create_model()
        self.model.fit(X_scaled, y)
        
        if all_metrics:
            avg_metrics = {
                'accuracy': np.mean([m['accuracy'] for m in all_metrics]),
                'precision': np.mean([m['precision'] for m in all_metrics]),
                'recall': np.mean([m['recall'] for m in all_metrics]),
                'f1': np.mean([m['f1'] for m in all_metrics]),
                'auc': np.mean([m['auc'] for m in all_metrics]) if all(m.get('auc') for m in all_metrics) else None,
            }
        else:
            y_pred = self.model.predict(X_scaled)
            y_prob = self.model.predict_proba(X_scaled)[:, 1]
            avg_metrics = self._calculate_metrics(y, y_pred, y_prob)
        
        self.training_metrics = avg_metrics
        self.model_version = datetime.now().strftime('%Y%m%d_%H%M%S')
        
        logger.info(f"Training complete. Avg metrics: {avg_metrics}")
        
        return {
            'success': True,
            'model_version': self.model_version,
            'metrics': avg_metrics,
            'split_metrics': all_metrics,
            'feature_importance': self._get_feature_importance(feature_names),
            'training_samples': len(y),
            'walk_forward_splits': len(all_metrics)
        }
    
    def _create_model(self):
        """Create a new model instance based on model_type."""
        if self.model_type == 'lightgbm':
            try:
                import lightgbm as lgb
                return lgb.LGBMClassifier(
                    n_estimators=100,
                    max_depth=5,
                    learning_rate=0.1,
                    random_state=42,
                    verbose=-1
                )
            except ImportError:
                logger.warning("LightGBM not installed, falling back to LogisticRegression")
        
        return LogisticRegression(
            max_iter=1000,
            random_state=42,
            class_weight='balanced'
        )
    
    def _calculate_metrics(self, y_true: np.ndarray, y_pred: np.ndarray, 
                          y_prob: np.ndarray) -> Dict[str, float]:
        """Calculate classification metrics."""
        metrics = {
            'accuracy': accuracy_score(y_true, y_pred),
            'precision': precision_score(y_true, y_pred, zero_division=0),
            'recall': recall_score(y_true, y_pred, zero_division=0),
            'f1': f1_score(y_true, y_pred, zero_division=0),
        }
        
        if len(np.unique(y_true)) > 1:
            try:
                metrics['auc'] = roc_auc_score(y_true, y_prob)
            except:
                metrics['auc'] = None
        
        return metrics
    
    def _get_feature_importance(self, feature_names: List[str]) -> Dict[str, float]:
        """Get feature importance from trained model."""
        if self.model is None:
            return {}
        
        if hasattr(self.model, 'feature_importances_'):
            importances = self.model.feature_importances_
        elif hasattr(self.model, 'coef_'):
            importances = np.abs(self.model.coef_[0])
        else:
            return {}
        
        importance_dict = dict(zip(feature_names, importances))
        return dict(sorted(importance_dict.items(), key=lambda x: -x[1]))
    
    def predict(self, features: Dict[str, Any]) -> Dict[str, Any]:
        """
        Predict P(TP100) for a single anomaly.
        
        Args:
            features: Dict with feature values
        
        Returns:
            Dict with predicted probability and confidence
        """
        if self.model is None:
            return {'error': 'Model not trained'}
        
        feature_vector = []
        for col in self.feature_columns:
            val = features.get(col, 0)
            if val is None:
                val = 0
            if isinstance(val, bool):
                val = int(val)
            feature_vector.append(float(val))
        
        X = np.array([feature_vector])
        X_scaled = self.scaler.transform(X)
        
        prob = self.model.predict_proba(X_scaled)[0, 1]
        pred = int(prob >= 0.5)
        
        return {
            'predicted_tp100_prob': round(prob, 4),
            'predicted_class': pred,
            'model_version': self.model_version
        }
    
    def predict_batch(self, anomalies: List[Dict]) -> List[Dict]:
        """
        Predict P(TP100) for multiple anomalies.
        
        Args:
            anomalies: List of anomaly dicts with features
        
        Returns:
            List of prediction dicts
        """
        if self.model is None:
            return [{'error': 'Model not trained'} for _ in anomalies]
        
        results = []
        for anomaly in anomalies:
            pred = self.predict(anomaly)
            results.append(pred)
        
        return results
    
    def save(self, filename: Optional[str] = None) -> str:
        """
        Save the trained model to disk.
        
        Args:
            filename: Optional filename (default: tp100_model_{version}.pkl)
        
        Returns:
            Path to saved model file
        """
        if self.model is None:
            raise ValueError("No model to save - train first")
        
        if filename is None:
            filename = f"tp100_model_{self.model_version}.pkl"
        
        filepath = MODEL_DIR / filename
        
        model_data = {
            'model': self.model,
            'scaler': self.scaler,
            'model_type': self.model_type,
            'model_version': self.model_version,
            'feature_columns': self.feature_columns,
            'training_metrics': self.training_metrics,
            'saved_at': datetime.now().isoformat()
        }
        
        with open(filepath, 'wb') as f:
            pickle.dump(model_data, f)
        
        logger.info(f"Model saved to {filepath}")
        return str(filepath)
    
    def load(self, filepath: Optional[str] = None) -> bool:
        """
        Load a trained model from disk.
        
        Args:
            filepath: Path to model file (default: latest in MODEL_DIR)
        
        Returns:
            True if loaded successfully
        """
        if filepath is None:
            model_files = list(MODEL_DIR.glob('tp100_model_*.pkl'))
            if not model_files:
                logger.warning("No saved models found")
                return False
            filepath = max(model_files, key=lambda p: p.stat().st_mtime)
        else:
            filepath = Path(filepath)
        
        if not filepath.exists():
            logger.error(f"Model file not found: {filepath}")
            return False
        
        with open(filepath, 'rb') as f:
            model_data = pickle.load(f)
        
        self.model = model_data['model']
        self.scaler = model_data['scaler']
        self.model_type = model_data['model_type']
        self.model_version = model_data['model_version']
        self.feature_columns = model_data['feature_columns']
        self.training_metrics = model_data['training_metrics']
        
        logger.info(f"Model loaded from {filepath} (version: {self.model_version})")
        return True


def train_and_save_model():
    """Train a new model and save it."""
    model = TP100Model(model_type='logistic')
    results = model.train()
    
    if results.get('success'):
        model.save()
        print("\nTraining Results:")
        print(f"  Model Version: {results['model_version']}")
        print(f"  Training Samples: {results['training_samples']}")
        print(f"  Walk-Forward Splits: {results['walk_forward_splits']}")
        print(f"\nMetrics:")
        for metric, value in results['metrics'].items():
            if value is not None:
                print(f"  {metric}: {value:.4f}")
        print(f"\nTop Feature Importance:")
        for i, (feature, importance) in enumerate(list(results['feature_importance'].items())[:10]):
            print(f"  {i+1}. {feature}: {importance:.4f}")
    else:
        print(f"Training failed: {results.get('error')}")
    
    return results


def main():
    """Main entry point."""
    import argparse
    
    parser = argparse.ArgumentParser(description='TP100 Probability Model')
    parser.add_argument('--train', action='store_true', help='Train a new model')
    parser.add_argument('--model-type', choices=['logistic', 'lightgbm'], 
                        default='logistic', help='Model type to use')
    args = parser.parse_args()
    
    if args.train:
        model = TP100Model(model_type=args.model_type)
        results = model.train()
        if results.get('success'):
            model.save()
            print(f"Model trained and saved: {results['model_version']}")
        else:
            print(f"Training failed: {results.get('error')}")
    else:
        print("Use --train to train a new model")


if __name__ == '__main__':
    main()
