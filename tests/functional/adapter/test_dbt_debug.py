from dbt.tests.util import run_dbt, run_dbt_and_capture
from dbt.tests.adapter.dbt_debug.test_dbt_debug import BaseDebug
import re

class TestDebugTeradata(BaseDebug):
    def test_ok(self, project):
        run_dbt(["debug"])
        assert "ERROR" not in self.capsys.readouterr().out

    def test_connection_flag(self, project):
        """Testing that the --connection flag works as expected, including that output is not lost"""
        _, out = run_dbt_and_capture(["debug", "--connection"])
        assert "Skipping steps before connection verification" in out

        _, out = run_dbt_and_capture(
            ["debug", "--connection", "--target", "NONE"], expect_pass=False
        )
        assert "1 check failed" in out
        assert "The profile 'test' does not have a target named 'NONE'." in out

        _, out = run_dbt_and_capture(
            ["debug", "--connection", "--profiles-dir", "NONE"], expect_pass=False
        )
        assert "Using profiles dir at NONE"
        assert "1 check failed" in out
        assert "dbt looked for a profiles.yml file in NONE" in out

    def test_nopass(self, project):
        run_dbt(["debug", "--target", "nopass"], expect_pass=False)
        self.assertGotValue(re.compile(r"\s+profiles\.yml file"), "ERROR invalid")

    def test_wronguser(self, project):
        run_dbt(["debug", "--target", "wronguser"], expect_pass=False)
        self.assertGotValue(re.compile(r"\s+Connection test"), "ERROR")

    def test_empty_target(self, project):
        run_dbt(["debug", "--target", "none_target"], expect_pass=False)
        self.assertGotValue(re.compile(r"\s+output 'none_target'"), "misconfigured")