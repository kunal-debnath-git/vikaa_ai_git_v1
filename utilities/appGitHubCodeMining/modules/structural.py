"""AST-based structural search helper.

Currently provides a simple utility to find function names with N args.
Future: expand to classes, decorators, call graphs, language-agnostic parsers.
"""
import ast

class Structural:
    def find_functions_with_args(self, code: str, arg_count: int):
        """Return function names in `code` that take exactly `arg_count` positional args."""
        tree = ast.parse(code)
        out = []
        for node in ast.walk(tree):
            if isinstance(node, ast.FunctionDef) and len(node.args.args) == arg_count:
                out.append(node.name)
        return out
