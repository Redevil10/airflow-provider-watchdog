"""Tests for alert dispatching."""

from __future__ import annotations

import json
from unittest.mock import MagicMock, patch

from airflow_watchdog.alerting import dispatch
from airflow_watchdog.config import WatchdogConfig
from airflow_watchdog.detectors import Alert, AlertType, Severity


def _make_alert(severity=Severity.WARNING, dag_id="test_dag", task_id=None):
    return Alert(
        alert_type=AlertType.RUNTIME_ANOMALY,
        severity=severity,
        dag_id=dag_id,
        task_id=task_id,
        message="Test alert message",
    )


class TestDispatch:
    def test_no_alerts_does_nothing(self):
        """dispatch() with empty list should not send email or slack."""
        config = WatchdogConfig(
            alert_emails=["test@example.com"],
            alert_slack_webhook="https://hooks.slack.com/test",
        )
        # Should not raise
        dispatch([], config)

    def test_logs_alerts(self, caplog):
        """Alerts are always logged."""
        import logging

        with caplog.at_level(logging.WARNING, logger="airflow_watchdog.alerting"):
            dispatch([_make_alert()], WatchdogConfig())

        assert "1 alert(s)" in caplog.text

    def test_email_sent_when_configured(self):
        """Email is dispatched when alert_emails is set."""
        config = WatchdogConfig(alert_emails=["team@example.com"])
        alert = _make_alert()

        mock_send = MagicMock()
        with patch.dict(
            "sys.modules",
            {
                "airflow.utils.email": MagicMock(send_email=mock_send),
                "airflow.utils": MagicMock(),
                "airflow": MagicMock(),
            },
        ):
            dispatch([alert], config)

        mock_send.assert_called_once()
        call_kwargs = mock_send.call_args
        assert "team@example.com" in call_kwargs.kwargs.get(
            "to", call_kwargs.args[0] if call_kwargs.args else []
        )

    def test_email_not_sent_when_unconfigured(self):
        """No email sent when alert_emails is empty."""
        config = WatchdogConfig(alert_emails=[])
        mock_send = MagicMock()
        with patch.dict(
            "sys.modules",
            {
                "airflow.utils.email": MagicMock(send_email=mock_send),
                "airflow.utils": MagicMock(),
                "airflow": MagicMock(),
            },
        ):
            dispatch([_make_alert()], config)
        mock_send.assert_not_called()

    def test_slack_sent_when_configured(self):
        """Slack webhook is called when configured."""
        config = WatchdogConfig(alert_slack_webhook="https://hooks.slack.com/test")
        alert = _make_alert()

        with patch("airflow_watchdog.alerting.urlopen") as mock_urlopen:
            mock_urlopen.return_value = MagicMock()
            dispatch([alert], config)

        mock_urlopen.assert_called_once()
        req = mock_urlopen.call_args.args[0]
        payload = json.loads(req.data)
        assert "Watchdog" in payload["text"]

    def test_slack_not_sent_when_unconfigured(self):
        """No slack notification when webhook is None."""
        config = WatchdogConfig(alert_slack_webhook=None)
        with patch("airflow_watchdog.alerting.urlopen") as mock_urlopen:
            dispatch([_make_alert()], config)
        mock_urlopen.assert_not_called()

    def test_email_failure_does_not_raise(self):
        """Email failure is logged but does not propagate."""
        config = WatchdogConfig(alert_emails=["team@example.com"])

        mock_send = MagicMock(side_effect=Exception("SMTP down"))
        with patch.dict(
            "sys.modules",
            {
                "airflow.utils.email": MagicMock(send_email=mock_send),
                "airflow.utils": MagicMock(),
                "airflow": MagicMock(),
            },
        ):
            # Should not raise
            dispatch([_make_alert()], config)

    def test_slack_failure_does_not_raise(self):
        """Slack failure is logged but does not propagate."""
        config = WatchdogConfig(alert_slack_webhook="https://hooks.slack.com/test")

        with patch("airflow_watchdog.alerting.urlopen", side_effect=Exception("Network error")):
            dispatch([_make_alert()], config)

    def test_critical_alerts_logged_at_critical_level(self, caplog):
        """Critical alerts use logger.critical."""
        import logging

        with caplog.at_level(logging.CRITICAL, logger="airflow_watchdog.alerting"):
            dispatch([_make_alert(severity=Severity.CRITICAL)], WatchdogConfig())

        assert "CRITICAL" in caplog.text

    def test_slack_caps_at_20_alerts(self):
        """Slack message truncates after 20 alerts."""
        config = WatchdogConfig(alert_slack_webhook="https://hooks.slack.com/test")
        alerts = [_make_alert(dag_id=f"dag_{i}") for i in range(25)]

        with patch("airflow_watchdog.alerting.urlopen") as mock_urlopen:
            mock_urlopen.return_value = MagicMock()
            dispatch(alerts, config)

        req = mock_urlopen.call_args.args[0]
        payload = json.loads(req.data)
        assert "5 more" in payload["text"]

    def test_teams_sent_when_configured(self):
        """MS Teams webhook is called when configured."""
        config = WatchdogConfig(alert_teams_webhook="https://outlook.office.com/webhook/test")
        alert = _make_alert()

        with patch("airflow_watchdog.alerting.urlopen") as mock_urlopen:
            mock_urlopen.return_value = MagicMock()
            dispatch([alert], config)

        mock_urlopen.assert_called_once()
        req = mock_urlopen.call_args.args[0]
        payload = json.loads(req.data)
        assert payload["type"] == "message"
        card = payload["attachments"][0]["content"]
        assert card["type"] == "AdaptiveCard"
        assert "1 alert(s)" in card["body"][0]["text"]

    def test_teams_not_sent_when_unconfigured(self):
        """No Teams notification when webhook is None."""
        config = WatchdogConfig(alert_teams_webhook=None)
        with patch("airflow_watchdog.alerting.urlopen") as mock_urlopen:
            dispatch([_make_alert()], config)
        mock_urlopen.assert_not_called()

    def test_teams_failure_does_not_raise(self):
        """Teams failure is logged but does not propagate."""
        config = WatchdogConfig(alert_teams_webhook="https://outlook.office.com/webhook/test")
        with patch(
            "airflow_watchdog.alerting.urlopen",
            side_effect=Exception("Network error"),
        ):
            dispatch([_make_alert()], config)

    def test_teams_caps_at_20_alerts(self):
        """Teams message truncates after 20 alerts."""
        config = WatchdogConfig(alert_teams_webhook="https://outlook.office.com/webhook/test")
        alerts = [_make_alert(dag_id=f"dag_{i}") for i in range(25)]

        with patch("airflow_watchdog.alerting.urlopen") as mock_urlopen:
            mock_urlopen.return_value = MagicMock()
            dispatch(alerts, config)

        req = mock_urlopen.call_args.args[0]
        payload = json.loads(req.data)
        facts = payload["attachments"][0]["content"]["body"][1]["facts"]
        assert len(facts) == 21  # 20 alerts + "...and 5 more"
