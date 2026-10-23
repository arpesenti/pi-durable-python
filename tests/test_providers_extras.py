"""The provider extras stay independent of each other and of the core package.

The adapters import their SDK only when a real client is needed, so the core install carries no
provider dependency and each extra can be installed alone. These cases run in subprocesses so the
SDK modules this environment already has cannot mask a leaking import.
"""

from __future__ import annotations

import os
import subprocess
import sys
import textwrap
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def run(code: str, *, extra_env: dict[str, str] | None = None) -> subprocess.CompletedProcess[str]:
    env = {**os.environ, "PYTHONPATH": str(ROOT / "src")}
    env.pop("OPENAI_API_KEY", None)
    env.pop("ANTHROPIC_API_KEY", None)
    env.update(extra_env or {})
    return subprocess.run(
        [sys.executable, "-c", textwrap.dedent(code)],
        capture_output=True,
        text=True,
        env=env,
        timeout=60,
    )


def test_adapter_modules_import_without_either_sdk() -> None:
    result = run(
        """
        import sys
        sys.modules["openai"] = None
        sys.modules["anthropic"] = None
        from pi_durable.providers import Model
        from pi_durable.providers.anthropic import AnthropicModels
        from pi_durable.providers.openai import OpenAIModels

        class Client:
            pass

        model = Model(provider="probe", model_id="probe-1")
        assert OpenAIModels([model], client=Client()).get_model("probe", "probe-1") is model
        assert AnthropicModels([model], client=Client()).get_model("probe", "probe-1") is model
        print("ok")
        """
    )
    assert result.returncode == 0, result.stderr
    assert result.stdout.strip() == "ok"


def test_a_missing_openai_extra_names_its_install_command() -> None:
    result = run(
        """
        import sys
        sys.modules["openai"] = None
        from pi_durable.providers import Model
        from pi_durable.providers.openai import OpenAIModels

        model = Model(provider="openai", model_id="probe-1")
        models = OpenAIModels([model])
        try:
            models.stream_simple(model, {"messages": []})
        except ImportError as error:
            assert "openai" in str(error) and "extra" in str(error), error
            print("ok")
        else:
            raise AssertionError("a missing extra must not be ignored")
        """,
        extra_env={"OPENAI_API_KEY": "test-key"},
    )
    assert result.returncode == 0, result.stderr
    assert result.stdout.strip() == "ok"


def test_a_missing_anthropic_extra_names_its_install_command() -> None:
    result = run(
        """
        import sys
        sys.modules["anthropic"] = None
        from pi_durable.providers import Model
        from pi_durable.providers.anthropic import AnthropicModels

        model = Model(provider="anthropic", model_id="probe-1")
        models = AnthropicModels([model])
        try:
            models.stream_simple(model, {"messages": []})
        except ImportError as error:
            assert "anthropic" in str(error) and "extra" in str(error), error
            print("ok")
        else:
            raise AssertionError("a missing extra must not be ignored")
        """,
        extra_env={"ANTHROPIC_API_KEY": "test-key"},
    )
    assert result.returncode == 0, result.stderr
    assert result.stdout.strip() == "ok"


def test_the_core_package_imports_without_either_sdk() -> None:
    result = run(
        """
        import sys
        sys.modules["openai"] = None
        sys.modules["anthropic"] = None
        import pi_durable
        from pi_durable.providers import Model, Models
        assert Models is not None
        assert Model is not None
        print("ok")
        """
    )
    assert result.returncode == 0, result.stderr
    assert result.stdout.strip() == "ok"
