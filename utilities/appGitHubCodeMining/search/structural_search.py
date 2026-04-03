# Structural Code Search (AST based pattern matching)

import ast

class StructuralCodeSearcher:
    def __init__(self):
        pass

    def find_functions_with_args(self, code, arg_count):
        tree = ast.parse(code)
        results = []
        for node in ast.walk(tree):
            if isinstance(node, ast.FunctionDef):
                if len(node.args.args) == arg_count:
                    results.append(node.name)
        return results

# Example Usage
if __name__ == "__main__":
    code = '''
def add(a, b):
    return a + b

def single(x):
    return x
'''
    searcher = StructuralCodeSearcher()
    print("Functions with 2 args:", searcher.find_functions_with_args(code, 2))