#!/usr/bin/env python3
"""
Email Notification System for High Conviction Anomaly Alerts

Two-tier architecture:
- TIER 1 (Event scoring): volume_score, z_score, vol_oi_score, magnitude — gates alerts
- TIER 2 (Contract selection): max_volume selection — picks recommended option

Sends email notifications when event-level score >= 3/4 and magnitude >= $20K.
"""

import os
import smtplib
import logging
from datetime import datetime, date
from email.mime.text import MIMEText as MimeText
from email.mime.multipart import MIMEMultipart as MimeMultipart
from typing import Dict, List, Any, Optional

logger = logging.getLogger(__name__)

# Try to import enrichment module
try:
    from enrichment.signal_enrichment import SignalEnrichment
    ENRICHMENT_AVAILABLE = True
except ImportError:
    ENRICHMENT_AVAILABLE = False
    logger.info("Enrichment module not available - alerts will send without enrichment context")

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
        
    def send_anomaly_alert(self, anomalies: Dict[str, Dict],
                           enrichment_data: Optional[Dict[str, Dict]] = None) -> bool:
        """Send email alert for detected anomalies.

        Sends email when event-level score >= 3/4 (volume_score, z_score, vol_oi_score, magnitude).
        Bot-driven and earnings-related events are excluded.

        If enrichment_data is provided, uses it directly. Otherwise computes enrichment on-the-fly.
        """
        if not self.enabled:
            logger.info("Email notifications disabled")
            return True

        if not anomalies:
            logger.info("No anomalies to report")
            return True

        BOT_THRESHOLD_PCT = 5.0

        # Filter for high conviction alerts (event score >= 3)
        high_conviction_alerts = {}
        bot_filtered_count = 0

        for symbol, data in anomalies.items():
            if data.get('total_magnitude', 0) < 20000:
                continue

            intraday_move = abs(data.get('intraday_price_move_pct', 0))
            if intraday_move >= BOT_THRESHOLD_PCT:
                logger.info(f"Excluding {symbol}: intraday move {intraday_move:.1f}% >= {BOT_THRESHOLD_PCT}% (likely bot-driven)")
                bot_filtered_count += 1
                continue

            # Event score >= 3 gates alerts
            if data.get('is_high_conviction', False):
                high_conviction_alerts[symbol] = data
                logger.info(
                    f"HIGH CONVICTION ALERT: {symbol} - Event score {data.get('high_conviction_score', 0)}/4, "
                    f"Recommended: {data.get('recommended_option', 'N/A')}"
                )

        if bot_filtered_count > 0:
            logger.info(f"Filtered out {bot_filtered_count} bot-driven anomalies (>={BOT_THRESHOLD_PCT}% intraday move)")

        if not high_conviction_alerts:
            logger.info("No high conviction alerts - no email sent")
            return False

        logger.info(f"Found {len(high_conviction_alerts)} HIGH CONVICTION alerts - sending email!")

        # Enrich alerts with external context (news, EDGAR, novelty)
        # If enrichment_data was pre-computed by the pipeline, use it directly
        if enrichment_data is None:
            enrichment_data = {}
        if not enrichment_data and ENRICHMENT_AVAILABLE:
            try:
                enricher = SignalEnrichment(skip_edgar=False, skip_news=False)
                today = date.today()
                for symbol, data in high_conviction_alerts.items():
                    event_date = data.get('event_date', today)
                    if isinstance(event_date, str):
                        try:
                            event_date = datetime.strptime(event_date, '%Y-%m-%d').date()
                        except ValueError:
                            event_date = today
                    elif isinstance(event_date, datetime):
                        event_date = event_date.date()

                    details = data.get('details', {})
                    call_vol = details.get('call_volume', 0)
                    put_vol = details.get('put_volume', 0)
                    direction = 'call_heavy' if call_vol > put_vol else 'put_heavy'

                    enrichment = enricher.enrich_event(symbol, event_date, direction)
                    enrichment_data[symbol] = enrichment
                    logger.info(f"Enrichment for {symbol}: conviction modifier={enrichment.get('conviction_modifiers', {}).get('net_modifier', 'N/A')}")
            except Exception as e:
                logger.warning(f"Enrichment failed (alerts will send without context): {e}")

        try:
            alert_count = len(high_conviction_alerts)
            subject = f"[{alert_count} HIGH CONVICTION] INSIDER TRADING ALERT"

            html_content = self._create_email_content(high_conviction_alerts, enrichment_data)

            self._send_email(subject, html_content)
            logger.info(f"Email alert sent for {alert_count} high conviction alerts")
            return True

        except Exception as e:
            logger.error(f"Failed to send email alert: {e}")
            return False

    def _create_email_content(self, alerts: Dict[str, Dict],
                             enrichment_data: Optional[Dict[str, Dict]] = None) -> str:
        """Create HTML email content for high conviction alerts."""
        enrichment_data = enrichment_data or {}
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S EST')

        sorted_alerts = sorted(
            alerts.items(),
            key=lambda x: (-x[1].get('high_conviction_score', 0), -x[1].get('total_magnitude', 0))
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
                .factor-met {{ color: #2e7d32; }}
                .factor-miss {{ color: #999; }}
                .details {{ margin: 20px 0; padding: 15px; border: 1px solid #ddd; border-radius: 5px; background-color: #fafafa; }}
                .enrichment {{ margin: 10px 0; padding: 12px; border-left: 4px solid #2196F3; background-color: #e3f2fd; border-radius: 3px; font-size: 13px; }}
                .enrichment-high {{ border-left-color: #f44336; background-color: #ffebee; }}
                .enrichment-low {{ border-left-color: #9e9e9e; background-color: #f5f5f5; }}
                .footer {{ margin-top: 30px; font-size: 12px; color: #666; }}
            </style>
        </head>
        <body>
            <div class="header">
                <h1>HIGH CONVICTION INSIDER TRADING ALERT</h1>
                <p>Event Scoring + Greeks Contract Selection | {timestamp}</p>
            </div>

            <div class="summary">
                <h2>{len(sorted_alerts)} High Conviction Alert(s)</h2>
                <p><strong>Strategy:</strong> Exit at +100% gain, or hold to expiration</p>
                <p><strong>Event scoring:</strong> Volume anomaly, Z-score, Vol:OI ratio, Magnitude (3+ of 4 must exceed thresholds)</p>
                <p><strong>Contract selection:</strong> Highest-gamma tradeable contract (best delta exposure)</p>
                <p><strong>Filters:</strong> Not bot-driven (&lt;5% intraday move), not earnings-related</p>
                <p><strong>Dashboard:</strong> <a href="https://bk-insidertrades.streamlit.app">https://bk-insidertrades.streamlit.app</a></p>
            </div>

            <table class="alert-table">
                <tr>
                    <th>Symbol</th>
                    <th>Direction</th>
                    <th>Event Score</th>
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

            vol_score = details.get('volume_score', 0)
            z_score = details.get('z_score', 0)
            voi_score = details.get('volume_oi_ratio_score', 0)
            mag = details.get('total_magnitude', 0)

            def factor_badge(met, label, value):
                css = "factor-met" if met else "factor-miss"
                check = "&#10003;" if met else "&#10007;"
                return f'<span class="{css}">{check} {label}: {value}</span>'

            html += f"""
            <div class="details">
                <h3>{symbol} - {direction}</h3>
                <p><strong>Event Score:</strong> {data.get('high_conviction_score', 0)}/4</p>
                <p><strong>Event Factors:</strong><br>
                    {factor_badge(vol_score >= 2.0, 'Volume Score', f'{vol_score:.1f}')}<br>
                    {factor_badge(z_score >= 3.0, 'Z-Score', f'{z_score:.1f}')}<br>
                    {factor_badge(voi_score >= 1.2, 'Vol:OI Score', f'{voi_score:.1f}')}<br>
                    {factor_badge(mag >= 50000, 'Magnitude', f'${mag:,.0f}')}
                </p>
                <p><strong>Recommended Option:</strong> <code>{data.get('recommended_option', 'N/A')}</code>
                   (selection: {data.get('contract_selection_strategy', 'max_volume')})</p>
                <p><strong>Magnitude:</strong> ${details.get('total_magnitude', 0):,.0f}
                   (Call: ${details.get('call_magnitude', 0):,.0f}, Put: ${details.get('put_magnitude', 0):,.0f})</p>
                <p><strong>Volume:</strong> {call_vol + put_vol:,} contracts
                   (Call: {call_vol:,}, Put: {put_vol:,})</p>
            """

            # Add enrichment context if available
            enrichment = enrichment_data.get(symbol)
            if enrichment:
                net_mod = enrichment.get('conviction_modifiers', {}).get('net_modifier', 0)
                css_class = 'enrichment-high' if net_mod >= 2 else 'enrichment-low' if net_mod <= -1 else ''
                html += f'<div class="enrichment {css_class}">'
                html += '<strong>Signal Context:</strong><br>'

                if ENRICHMENT_AVAILABLE:
                    html += SignalEnrichment.format_for_email(enrichment)
                else:
                    # Manual formatting fallback
                    novelty = enrichment.get('novelty', {})
                    if novelty.get('is_first_trigger'):
                        html += 'FIRST-TIME TRIGGER - never seen anomaly on this ticker<br>'
                    elif novelty.get('trigger_count_30d', 0) <= 2:
                        html += f'Rare trigger - {novelty.get("trigger_count_30d", "?")}x in 30 days<br>'

                    news = enrichment.get('news', {})
                    if news.get('has_news') is False:
                        html += 'NO RECENT NEWS - possible information asymmetry<br>'
                    elif news.get('has_catalyst_news'):
                        html += f'Known catalyst: {", ".join(news.get("catalyst_keywords", [])[:3])}<br>'

                html += '</div>'

            html += "</div>"

        html += """
            <div class="footer">
                <p><strong>DISCLAIMER:</strong> This alert is for informational purposes only.
                Detection of statistical anomalies does not constitute proof of insider trading or investment advice.</p>
                <p>Generated by Insider Trading Detection System (Two-Tier Architecture)</p>
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


def send_anomaly_notification(anomalies: Dict[str, Dict],
                              enrichment_data: Optional[Dict[str, Dict]] = None) -> bool:
    """Convenience function to send anomaly notifications."""
    notifier = EmailNotifier()
    return notifier.send_anomaly_alert(anomalies, enrichment_data=enrichment_data)


if __name__ == '__main__':
    notifier = EmailNotifier()
    if notifier.test_connection():
        print("Email configuration is working correctly")
    else:
        print("Email configuration test failed")
