"""
Migration: Add Multi-Contract Scoring Columns
Date: 2026-03-09
Purpose: Support scoring multiple contracts per symbol instead of just one recommended_option

New columns:
- contract_candidates: JSON array of top N scored contracts with their metrics
- best_gamma_contract: Contract ticker with highest gamma
- best_theta_contract: Contract ticker with highest theta
- best_rr_contract: Contract ticker with best risk/reward ratio
- signal_persistence_count: Number of consecutive snapshots with high conviction signal
- predicted_tp100_prob: Model-predicted probability of hitting TP100
- contract_selection_strategy: Which strategy was used to select the recommended option
"""

import os
import sys
import logging

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from database.core.connection import db

logger = logging.getLogger(__name__)


def up():
    """Add multi-contract scoring columns to daily_anomaly_snapshot."""
    print("Adding multi-contract scoring columns to daily_anomaly_snapshot...")
    
    conn = db.connect()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                ALTER TABLE daily_anomaly_snapshot
                ADD COLUMN IF NOT EXISTS contract_candidates JSONB DEFAULT NULL
            """)
            print("[OK] Added contract_candidates column (JSONB)")
            
            cur.execute("""
                ALTER TABLE daily_anomaly_snapshot
                ADD COLUMN IF NOT EXISTS best_gamma_contract VARCHAR(50) DEFAULT NULL
            """)
            print("[OK] Added best_gamma_contract column")
            
            cur.execute("""
                ALTER TABLE daily_anomaly_snapshot
                ADD COLUMN IF NOT EXISTS best_theta_contract VARCHAR(50) DEFAULT NULL
            """)
            print("[OK] Added best_theta_contract column")
            
            cur.execute("""
                ALTER TABLE daily_anomaly_snapshot
                ADD COLUMN IF NOT EXISTS best_rr_contract VARCHAR(50) DEFAULT NULL
            """)
            print("[OK] Added best_rr_contract column")
            
            cur.execute("""
                ALTER TABLE daily_anomaly_snapshot
                ADD COLUMN IF NOT EXISTS signal_persistence_count INTEGER DEFAULT 1
            """)
            print("[OK] Added signal_persistence_count column")
            
            cur.execute("""
                ALTER TABLE daily_anomaly_snapshot
                ADD COLUMN IF NOT EXISTS predicted_tp100_prob DECIMAL(5,4) DEFAULT NULL
            """)
            print("[OK] Added predicted_tp100_prob column")
            
            cur.execute("""
                ALTER TABLE daily_anomaly_snapshot
                ADD COLUMN IF NOT EXISTS contract_selection_strategy VARCHAR(30) DEFAULT 'max_volume'
            """)
            print("[OK] Added contract_selection_strategy column")
            
            cur.execute("""
                COMMENT ON COLUMN daily_anomaly_snapshot.contract_candidates IS 
                'JSON array of top N contract candidates with scores: [{ticker, gamma, theta, vega, moneyness, score, rank_volume, rank_gamma, rank_rr}]'
            """)
            
            cur.execute("""
                COMMENT ON COLUMN daily_anomaly_snapshot.best_gamma_contract IS 
                'Contract ticker with highest gamma among tradeable contracts'
            """)
            
            cur.execute("""
                COMMENT ON COLUMN daily_anomaly_snapshot.best_theta_contract IS 
                'Contract ticker with highest absolute theta among tradeable contracts'
            """)
            
            cur.execute("""
                COMMENT ON COLUMN daily_anomaly_snapshot.best_rr_contract IS 
                'Contract ticker with best risk/reward ratio (gamma * vega / |theta|)'
            """)
            
            cur.execute("""
                COMMENT ON COLUMN daily_anomaly_snapshot.signal_persistence_count IS 
                'Number of consecutive 15-min snapshots where this symbol had high conviction signal'
            """)
            
            cur.execute("""
                COMMENT ON COLUMN daily_anomaly_snapshot.predicted_tp100_prob IS 
                'Model-predicted probability of hitting +100% take profit (0.0000 to 1.0000)'
            """)
            
            cur.execute("""
                COMMENT ON COLUMN daily_anomaly_snapshot.contract_selection_strategy IS 
                'Strategy used to select recommended_option: max_volume, max_gamma, best_rr, atm_preference, model_ranked'
            """)
            print("[OK] Added column comments")
            
            cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_anomaly_persistence 
                ON daily_anomaly_snapshot (event_date, symbol, signal_persistence_count)
                WHERE signal_persistence_count >= 2
            """)
            print("[OK] Created index for persistence filter")
            
            cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_anomaly_predicted_prob 
                ON daily_anomaly_snapshot (event_date, predicted_tp100_prob DESC)
                WHERE predicted_tp100_prob >= 0.5
            """)
            print("[OK] Created index for model predictions")
            
            conn.commit()
            print("[OK] Migration completed successfully!")
            return True
            
    except Exception as e:
        conn.rollback()
        print(f"[ERROR] Migration failed: {e}")
        raise
    finally:
        conn.close()


def down():
    """Remove multi-contract scoring columns."""
    print("Removing multi-contract scoring columns...")
    
    conn = db.connect()
    try:
        with conn.cursor() as cur:
            cur.execute("DROP INDEX IF EXISTS idx_anomaly_persistence")
            cur.execute("DROP INDEX IF EXISTS idx_anomaly_predicted_prob")
            print("[OK] Dropped indexes")
            
            cur.execute("""
                ALTER TABLE daily_anomaly_snapshot
                DROP COLUMN IF EXISTS contract_candidates,
                DROP COLUMN IF EXISTS best_gamma_contract,
                DROP COLUMN IF EXISTS best_theta_contract,
                DROP COLUMN IF EXISTS best_rr_contract,
                DROP COLUMN IF EXISTS signal_persistence_count,
                DROP COLUMN IF EXISTS predicted_tp100_prob,
                DROP COLUMN IF EXISTS contract_selection_strategy
            """)
            print("[OK] Dropped columns")
            
            conn.commit()
            return True
            
    except Exception as e:
        conn.rollback()
        print(f"[ERROR] Rollback failed: {e}")
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser()
    parser.add_argument('--down', action='store_true')
    args = parser.parse_args()
    
    if args.down:
        down()
    else:
        up()
