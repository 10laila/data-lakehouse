"""Contrôles statiques sans Spark : organisation et syntaxe Python du projet."""
import ast
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


class StructureTests(unittest.TestCase):
    def test_scripts_are_valid_python(self):
        for path in (ROOT / "spark").rglob("*.py"):
            with self.subTest(path=path.relative_to(ROOT)):
                ast.parse(path.read_text(encoding="utf-8"), filename=str(path))

    def test_expected_entrypoints(self):
        for relative in (
            "spark/bronze/ingest_bronze.py",
            "spark/silver/run_silver.py",
            "spark/silver/clean_products.py",
            "spark/silver/clean_customers.py",
            "spark/silver/clean_orders.py",
            "spark/quality/profiling/profile_table.py",
            "spark/quality/checks/report_silver.py",
        ):
            with self.subTest(path=relative):
                self.assertTrue((ROOT / relative).is_file())

    def test_silver_entrypoint_calls_all_three_cleaners(self):
        tree = ast.parse((ROOT / "spark/silver/run_silver.py").read_text(encoding="utf-8"))
        called = {node.func.id for node in ast.walk(tree)
                  if isinstance(node, ast.Call) and isinstance(node.func, ast.Name)}
        self.assertTrue({"clean_customers", "clean_products", "clean_orders"}.issubset(called))


if __name__ == "__main__":
    unittest.main()
