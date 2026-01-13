#!/usr/bin/env python3
"""
Email Notification System for Greeks-Based High Conviction Alerts

Sends email notifications ONLY when Greeks-based high conviction alerts are detected.
Legacy composite score alerts are logged but do not trigger emails.
"""

import os
import smtplib
import logging
from datetime import datetime
from email.mime.text import MIMEText as MimeText
from email.mime.multipart import MIMEMultipart as MimeMultipart
from typing import Dict, List, Any

logger = logging.getLogger(__name__)

class EmailNotifier:
    def __init__(self):
        self.smtp_host = os.getenv('SMTP_SERVER', 'smtp.gmail.com')
        self.smtp_port = int(os.getenv('SMTP_PORT', '465'))
        self.smtp_user = os.getenv('SENDER_EMAIL', '')
        self.smtp_pass = os.getenv('EMAIL_PASSWORD', '')
        self.use_tls = self.smtp_port == 587
        self.use_ssl = self.smtp_port == 465
        self.from_email = os.getenv('SENDER_EMAIL', self.smtp_user)
        self.to_emails = self._parse_recipient_emails(os.getenv('RECIPIENT_EMAIL', ''))
        self.enabled = os.getenv('ANOMALY_EMAIL_ENABLED', 'true').lower() == 'true'
    
    def _parse_recipient_emails(self, recipient_string: str) -> List[str]:
        """Parse recipient email string into a list of email addresses."""
        if not recipient_string:
            return []
        
        emails = []
        for separator in [',', ';', '\n']:
            if separator in recipient_string:
                emails.extend(recipient_string.split(separator))
                break
        else:
            emails = recipient_string.split()
        
        cleaned_emails = []
        for email in emails:
            email = email.strip()
            if email and '@' in email:
                cleaned_emails.append(email)
        
        logger.info(f"Parsed {len(cleaned_emails)} recipient email(s)")
        return cleaned_emails
        
    def send_anomaly_alert(self, anomalies: Dict[str, Dict]) -> bool:
        """Send email alert for detected anomalies.
        
        ONLY sends email if there are Greeks-based high conviction alerts (score >= 3).
        Legacy composite score alerts are logged but do not trigger emails.
        """
        if not self.enabled:
            logger.info("Email notifications disabled")
            return True
            
        if not anomalies:
            logger.info("No anomalies to report")
            return True
        
        BOT_THRESHOLD_PCT = 5.0
            
        # Filter for Greeks-based high conviction alerts ONLY
        high_conviction_greeks_alerts = {}
        bot_filtered_count = 0
        legacy_only_count = 0
        
        for symbol, data in anomalies.items():
            if data.get('total_magnitude', 0) < 20000:
                continue
            
            intraday_move = abs(data.get('intraday_price_move_pct', 0))
            if intraday_move >= BOT_THRESHOLD_PCT:
                logger.info(f"Excluding {symbol}: intraday move {intraday_move:.1f}% >= {BOT_THRESHOLD_PCT}% (likely bot-driven)")
                bot_filtered_count += 1
                continue
            
            # ONLY include Greeks-based high conviction alerts (score >= 3)
            if data.get('is_high_conviction', False):
                high_conviction_greeks_alerts[symbol] = data
                logger.info(f"HIGH CONVICTION ALERT: {symbol} - Greeks score {data.get('high_conviction_score', 0)}/4, Recommended: {data.get('recommended_option', 'N/A')}")
            elif data.get('composite_score', 0) >= 7.5:
                legacy_only_count += 1
                logger.info(f"Legacy-only alert (no email): {symbol} - Composite score {data.get('composite_score', 0):.1f}")
        
        if bot_filtered_count > 0:
            logger.info(f"Filtered out {bot_filtered_count} bot-driven anomalies (>={BOT_THRESHOLD_PCT}% intraday move)")
        
        if legacy_only_count > 0:
            logger.info(f"Skipped {legacy_only_count} legacy-only alerts (not Greeks-based high conviction)")
        
        # ONLY send email if we have Greeks-based high conviction alerts
        if not high_conviction_greeks_alerts:
            logger.info("No Greeks-based high conviction alerts - no email sent")
            return False
        
        logger.info(f"Found {len(high_conviction_greeks_alerts)} HIGH CONVICTION (Greeks-based) alerts - sending email!")
            
        try:
            greeks_alerts_count = len(high_conviction_greeks_alerts)
            subject = f"[{greeks_alerts_count} HIGH CONVICTION] INSIDER TRADING ALERT"
            
            html_content = self._create_email_content(high_conviction_greeks_alerts)
            
            self._send_email(subject, html_content)
            logger.info(f"Email alert sent for {greeks_alerts_count} Greeks-based high conviction alerts")
            return True
            
        except Exception as e:
            logger.error(f"Failed to send email alert: {e}")
            return False

    def _create_email_content(self, alerts: Dict[str, Dict]) -> str:
        """Create HTML email content for Greeks-based high conviction alerts."""
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S EST')
        
        sorted_alerts = sorted(
            alerts.items(), 
            key=lambda x: -x[1].get('high_conviction_score', 0)
        )
        
        html = f"""
        <html>
        <head>
            <style>
                body {{ font-family: Arial, sans-serif; margin: 20px; }}
                .header {{ background-color: #ff9800; color: white; padding: 15px; border-radius: 5px; }}
                .summary {{ background-color: #fff3e0; padding: 15px; margin: 15px 0; border-radius: 5px; border: 1px solid #ff9800; }}
                .alert-table {{ border-collapse: collapse; width: 100%; margin: 15px 0; }}
                .alert-table th {{ background-color: #ff9800; color: white; padding: 12px; text-align: left; }}
                .alert-table td {{ border: 1px solid #ddd; padding: 10px; }}
                .bullish {{ color: #2e7d32; font-weight: bold; }}
                .bearish {{ color: #c62828; font-weight: bold; }}
                .details {{ margin: 20px 0; padding: 15px; border: 1px solid #ddd; border-radius: 5px; background-color: #fafafa; }}
                .footer {{ margin-top: 30px; font-size: 12px; color: #666; }}
            </style>
        </head>
        <body>
            <div class="header">
                <h1>HIGH CONVICTION INSIDER TRADING ALERT</h1>
                <p>Greeks-Based Scoring System | {timestamp}</p>
            </div>
            
            <div class="summary">
                <h2>{len(sorted_alerts)} High Conviction Alert(s)</h2>
                <p><strong>Strategy:</strong> Exit at +100% gain, or hold to expiration</p>
                <p><strong>Expected hit rate:</strong> ~50% for +100% returns</p>
                <p><strong>Scoring:</strong> Based on Theta, Gamma, Vega, OTM Score (93rd percentile thresholds)</p>
                <p><strong>Dashboard:</strong> <a href="https://bk-insidertrades.streamlit.app">https://bk-insidertrades.streamlit.app</a></p>
            </div>
            
            <table class="alert-table">
                <tr>
                    <th>Symbol</th>
                    <th>Direction</th>
                    <th>Greeks Score</th>
                    <th>Recommended Option</th>
                    <th>Magnitude</th>
                </tr>
        """
        
        for symbol, data in sorted_alerts:
            details = data.get('details', {})
            call_vol = details.get('call_volume', 0)
            put_vol = details.get('put_volume', 0)
            direction = "BULLISH" if call_vol > put_vol else "BEARISH"
            direction_class = "bullish" if direction == "BULLISH" else "bearish"
            magnitude = data.get('total_magnitude', 0)
            
            html += f"""
                <tr>
                    <td><strong>{symbol}</strong></td>
                    <td class="{direction_class}">{direction}</td>
                    <td>{data.get('high_conviction_score', 0)}/4</td>
                    <td><code>{data.get('recommended_option', 'N/A')}</code></td>
                    <td>${magnitude:,.0f}</td>
                </tr>
            """
        
        html += "</table>"
        
        # Detailed breakdown
        html += "<h2>Detailed Breakdown</h2>"
        
        for symbol, data in sorted_alerts:
            details = data.get('details', {})
            call_vol = details.get('call_volume', 0)
            put_vol = details.get('put_volume', 0)
            direction = "BULLISH" if call_vol > put_vol else "BEARISH"
            
            html += f"""
            <div class="details">
                <h3>{symbol} - {direction}</h3>
                <p><strong>Greeks Score:</strong> {data.get('high_conviction_score', 0)}/4</p>
                <p><strong>Recommended Option:</strong> <code>{data.get('recommended_option', 'N/A')}</code></p>
                <p><strong>Magnitude:</strong> ${details.get('total_magnitude', 0):,.0f} (Call: ${details.get('call_magnitude', 0):,.0f}, Put: ${details.get('put_magnitude', 0):,.0f})</p>
                <p><strong>Volume:</strong> {call_vol + put_vol:,} contracts (Call: {call_vol:,}, Put: {put_vol:,})</p>
                <p><strong>OTM Score:</strong> {details.get('otm_score', 0):.2f}</p>
            </div>
            """
        
        html += """
            <div class="footer">
                <p><strong>DISCLAIMER:</strong> This alert is for informational purposes only. 
                Detection of statistical anomalies does not constitute proof of insider trading or investment advice.</p>
                <p>Generated by Insider Trading Detection System</p>
            </div>
        </body>
        </html>
        """
        
        return html
    
    def _send_email(self, subject: str, html_content: str):
        """Send the email using SMTP."""
        if not self.smtp_user or not self.smtp_pass or not self.to_emails:
            raise ValueError("Missing required email configuration (SENDER_EMAIL, EMAIL_PASSWORD, RECIPIENT_EMAIL)")
        
        msg = MimeMultipart('alternative')
        msg['Subject'] = subject
        msg['From'] = self.from_email
        msg['To'] = self.from_email
        msg['Bcc'] = ', '.join(self.to_emails)
        
        html_part = MimeText(html_content, 'html')
        msg.attach(html_part)
        
        if self.use_ssl:
            with smtplib.SMTP_SSL(self.smtp_host, self.smtp_port) as server:
                server.login(self.smtp_user, self.smtp_pass)
                server.send_message(msg, to_addrs=[self.from_email] + self.to_emails)
        else:
            with smtplib.SMTP(self.smtp_host, self.smtp_port) as server:
                if self.use_tls:
                    server.starttls()
                server.login(self.smtp_user, self.smtp_pass)
                server.send_message(msg, to_addrs=[self.from_email] + self.to_emails)
    
    def test_connection(self) -> bool:
        """Test email configuration and connection."""
        try:
            if self.use_ssl:
                with smtplib.SMTP_SSL(self.smtp_host, self.smtp_port) as server:
                    server.login(self.smtp_user, self.smtp_pass)
            else:
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
    notifier = EmailNotifier()
    if notifier.test_connection():
        print("Email configuration is working correctly")
    else:
        print("Email configuration test failed")
