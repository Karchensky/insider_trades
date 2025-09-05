#!/usr/bin/env python3
"""
Email Notification System for High-Conviction Insider Trading Alerts

Sends detailed email notifications when anomalies are detected during intraday runs.
"""

import os
import smtplib
import logging
from datetime import datetime
from email.mime.text import MIMEText as MimeText
from email.mime.multipart import MIMEMultipart as MimeMultipart
from typing import Dict, List, Any
from decimal import Decimal

logger = logging.getLogger(__name__)

class EmailNotifier:
    def __init__(self):
        # Match your .env template variables
        self.smtp_host = os.getenv('SMTP_SERVER', 'smtp.gmail.com')
        self.smtp_port = int(os.getenv('SMTP_PORT', '465'))
        self.smtp_user = os.getenv('SENDER_EMAIL', '')
        self.smtp_pass = os.getenv('EMAIL_PASSWORD', '')
        self.use_tls = self.smtp_port == 587  # Use TLS for port 587, SSL for 465
        self.use_ssl = self.smtp_port == 465  # Use SSL for port 465
        self.from_email = os.getenv('SENDER_EMAIL', self.smtp_user)
        self.to_email = os.getenv('RECIPIENT_EMAIL', '')
        self.min_score = float(os.getenv('ANOMALY_ALERT_MIN_SCORE', '7.0'))
        self.enabled = os.getenv('ANOMALY_EMAIL_ENABLED', 'true').lower() == 'true'
        
    def send_anomaly_alert(self, anomalies: Dict[str, Dict]) -> bool:
        """Send email alert for detected anomalies."""
        if not self.enabled:
            logger.info("Email notifications disabled")
            return True
            
        if not anomalies:
            logger.info("No anomalies to report")
            return True
            
        # Filter anomalies by minimum score
        high_conviction_anomalies = {
            symbol: data for symbol, data in anomalies.items()
            if data.get('composite_score', 0) >= self.min_score
        }
        
        if not high_conviction_anomalies:
            logger.info(f"No anomalies above threshold {self.min_score}")
            return True
            
        try:
            # Create email content
            subject = f"INSIDER TRADING ALERT: {len(high_conviction_anomalies)} High-Conviction Anomalies Detected"
            html_content = self._create_email_content(high_conviction_anomalies)
            
            # Send email
            self._send_email(subject, html_content)
            logger.info(f"Email alert sent for {len(high_conviction_anomalies)} anomalies")
            return True
            
        except Exception as e:
            logger.error(f"Failed to send email alert: {e}")
            return False
    
    def _create_email_content(self, anomalies: Dict[str, Dict]) -> str:
        """Create HTML email content with anomaly details."""
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S EST')
        
        # Sort anomalies by score descending
        sorted_anomalies = sorted(
            anomalies.items(), 
            key=lambda x: x[1].get('composite_score', 0), 
            reverse=True
        )
        
        html = f"""
        <html>
        <head>
            <style>
                body {{ font-family: Arial, sans-serif; margin: 20px; }}
                .header {{ background-color: #d73027; color: white; padding: 15px; border-radius: 5px; }}
                .summary {{ background-color: #f8f9fa; padding: 15px; margin: 15px 0; border-radius: 5px; }}
                .anomaly {{ margin: 20px 0; padding: 15px; border: 1px solid #ddd; border-radius: 5px; }}
                .score {{ font-weight: bold; font-size: 18px; }}
                .high-score {{ color: #d73027; }}
                .indicators {{ margin: 10px 0; }}
                .indicator {{ margin: 5px 0; padding: 5px; background-color: #f1f3f4; border-radius: 3px; }}
                table {{ border-collapse: collapse; width: 100%; margin: 15px 0; }}
                th, td {{ border: 1px solid #ddd; padding: 8px; text-align: left; }}
                th {{ background-color: #f2f2f2; }}
                .footer {{ margin-top: 30px; font-size: 12px; color: #666; }}
            </style>
        </head>
        <body>
            <div class="header">
                <h1>HIGH-CONVICTION INSIDER TRADING ALERT</h1>
                <p>Detected at: {timestamp}</p>
            </div>
            
            <div class="summary">
                <h2>Alert Summary</h2>
                <p><strong>{len(sorted_anomalies)} symbols</strong> detected with high-conviction insider trading patterns (score ≥ {self.min_score}/10.0)</p>
                <p>These anomalies represent statistical outliers that warrant immediate investigation.</p>
            </div>
        """
        
        # Create summary table
        html += """
            <h2>Anomaly Summary Table</h2>
            <table>
                <tr>
                    <th>Symbol</th>
                    <th>Score</th>
                    <th>Key Indicators</th>
                    <th>Insider Pattern</th>
                </tr>
        """
        
        for symbol, data in sorted_anomalies:
            score = data.get('composite_score', 0)
            details = data.get('details', {})
            
            # Extract key indicators
            call_volume = details.get('call_volume', 0)
            put_volume = details.get('put_volume', 0)
            call_baseline = details.get('call_baseline_avg', 1)
            total_volume = call_volume + put_volume
            
            call_multiplier = call_volume / call_baseline if call_baseline > 0 else 0
            call_percentage = (call_volume / total_volume * 100) if total_volume > 0 else 0
            
            # Determine insider pattern
            if call_percentage >= 80:
                pattern = "Strong bullish insider activity"
            elif call_percentage <= 20:
                pattern = "Strong bearish insider activity"
            else:
                pattern = "Mixed directional positioning"
                
            key_indicators = f"""
                • {call_multiplier:.1f}x normal call volume<br/>
                • {call_percentage:.0f}% calls vs {100-call_percentage:.0f}% puts<br/>
                • OTM Score: {details.get('otm_call_score', 0):.1f}/3.0
            """
            
            html += f"""
                <tr>
                    <td><strong>{symbol}</strong></td>
                    <td class="high-score">{score:.1f}/10</td>
                    <td>{key_indicators}</td>
                    <td>{pattern}</td>
                </tr>
            """
        
        html += "</table>"
        
        # Detailed breakdown for each anomaly
        html += "<h2>Detailed Analysis</h2>"
        
        for symbol, data in sorted_anomalies:
            score = data.get('composite_score', 0)
            details = data.get('details', {})
            
            volume_score = details.get('volume_score', 0)
            otm_score = details.get('otm_call_score', 0)
            directional_score = details.get('directional_score', 0)
            time_score = details.get('time_pressure_score', 0)
            
            call_volume = details.get('call_volume', 0)
            put_volume = details.get('put_volume', 0)
            call_baseline = details.get('call_baseline_avg', 1)
            put_baseline = details.get('put_baseline_avg', 1)
            
            html += f"""
                <div class="anomaly">
                    <h3>{symbol} - <span class="score high-score">{score:.1f}/10.0</span></h3>
                    
                    <div class="indicators">
                        <h4>Score Breakdown:</h4>
                        <div class="indicator">Volume Anomaly: {volume_score:.1f}/3.0 points</div>
                        <div class="indicator">OTM Call Concentration: {otm_score:.1f}/3.0 points</div>
                        <div class="indicator">Directional Bias: {directional_score:.1f}/2.0 points</div>
                        <div class="indicator">Time Pressure: {time_score:.1f}/2.0 points</div>
                    </div>
                    
                    <div class="indicators">
                        <h4>Trading Activity:</h4>
                        <div class="indicator">Call Volume: {call_volume:,} (vs {call_baseline:.0f} baseline avg)</div>
                        <div class="indicator">Put Volume: {put_volume:,} (vs {put_baseline:.0f} baseline avg)</div>
                        <div class="indicator">Call Multiplier: {(call_volume/call_baseline if call_baseline > 0 else 0):.1f}x normal</div>
                        <div class="indicator">Total Volume: {call_volume + put_volume:,} contracts</div>
                    </div>
                    
                    <div class="indicators">
                        <h4>Insider Trading Indicators:</h4>
                        <div class="indicator">Statistical Significance: {volume_score:.1f}/3.0 (Z-score analysis)</div>
                        <div class="indicator">OTM Call Focus: {otm_score:.1f}/3.0 (Classic insider pattern)</div>
                        <div class="indicator">Directional Conviction: {directional_score:.1f}/2.0 (Call/put bias)</div>
                        <div class="indicator">Timing Urgency: {time_score:.1f}/2.0 (Near-term clustering)</div>
                    </div>
                </div>
            """
        
        html += f"""
            <div class="footer">
                <p><strong>IMPORTANT DISCLAIMER:</strong> This alert is for informational purposes only. 
                Detection of statistical anomalies does not constitute proof of insider trading or investment advice. 
                Always conduct proper due diligence and comply with all applicable laws and regulations.</p>
                
                <p>Generated by Insider Trading Detection System at {timestamp}</p>
                <p>For technical support or to modify alert settings, contact your system administrator.</p>
            </div>
        </body>
        </html>
        """
        
        return html
    
    def _send_email(self, subject: str, html_content: str):
        """Send the email using SMTP."""
        if not self.smtp_user or not self.smtp_pass or not self.to_email:
            raise ValueError("Missing required email configuration (SENDER_EMAIL, EMAIL_PASSWORD, RECIPIENT_EMAIL)")
        
        # Create message
        msg = MimeMultipart('alternative')
        msg['Subject'] = subject
        msg['From'] = self.from_email
        msg['To'] = self.to_email
        
        # Add HTML content
        html_part = MimeText(html_content, 'html')
        msg.attach(html_part)
        
        # Send email with proper SSL/TLS handling
        if self.use_ssl:
            # Use SMTP_SSL for port 465
            with smtplib.SMTP_SSL(self.smtp_host, self.smtp_port) as server:
                server.login(self.smtp_user, self.smtp_pass)
                server.send_message(msg)
        else:
            # Use SMTP with STARTTLS for port 587
            with smtplib.SMTP(self.smtp_host, self.smtp_port) as server:
                if self.use_tls:
                    server.starttls()
                server.login(self.smtp_user, self.smtp_pass)
                server.send_message(msg)
    
    def test_connection(self) -> bool:
        """Test email configuration and connection."""
        try:
            with smtplib.SMTP(self.smtp_host, self.smtp_port) as server:
                if self.use_tls:
                    server.starttls()
                server.login(self.smtp_user, self.smtp_pass)
                logger.info("Email connection test successful")
                return True
        except Exception as e:
            logger.error(f"Email connection test failed: {e}")
            return False


def send_anomaly_notification(anomalies: Dict[str, Dict]) -> bool:
    """Convenience function to send anomaly notifications."""
    notifier = EmailNotifier()
    return notifier.send_anomaly_alert(anomalies)


if __name__ == '__main__':
    # Test email configuration
    notifier = EmailNotifier()
    if notifier.test_connection():
        print("Email configuration is working correctly")
    else:
        print("Email configuration test failed")
