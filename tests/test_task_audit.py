import ast
from pathlib import Path

SOURCE_ROOT = Path(__file__).parents[1] / "src" / "lonelypsc"


def _python_files() -> list[Path]:
    return sorted(SOURCE_ROOT.rglob("*.py"))


def test_application_code_has_no_direct_task_cancellation() -> None:
    violations: list[str] = []
    for path in _python_files():
        tree = ast.parse(path.read_text(), filename=str(path))
        for node in ast.walk(tree):
            if not isinstance(node, ast.Call):
                continue
            if isinstance(node.func, ast.Attribute) and node.func.attr == "cancel":
                violations.append(f"{path}:{node.lineno}")

    assert not violations, "direct task cancellation found: " + ", ".join(violations)


def test_task_creation_is_concentrated_in_owned_task_factory() -> None:
    violations: list[str] = []
    for path in _python_files():
        tree = ast.parse(path.read_text(), filename=str(path))
        for node in ast.walk(tree):
            if not isinstance(node, ast.Call):
                continue
            if not (
                isinstance(node.func, ast.Attribute)
                and isinstance(node.func.value, ast.Name)
                and node.func.value.id == "asyncio"
                and node.func.attr == "create_task"
            ):
                continue
            if path.name != "task.py":
                violations.append(f"{path}:{node.lineno}")

    assert not violations, "unowned task creation found: " + ", ".join(violations)
