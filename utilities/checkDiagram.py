#!/usr/bin/env python3
# How to run: python E:\_vikaa-ai-v1\utilities\checkDiagram.py --outdir ./out --format png

"""
login_flow_diagrams.py
Generates “Login Flow” diagrams using Graphviz, Mermaid, and Diagrams.
Falls back to matplotlib-based proxies if Mermaid/Diagrams renderers aren’t available.
"""

import os
import shutil
import subprocess
from pathlib import Path
import sys

def ensure_outdir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)

# ---------------------------
# 1) GRAPHVIZ (true render)
# ---------------------------
def render_graphviz(outdir: Path, fmt: str = "png") -> Path:
    from graphviz import Digraph
    dot = Digraph("LoginFlow")
    dot.attr(rankdir="LR")
    dot.node("A", "User")
    dot.node("B", "Login Page")
    dot.node("C", "Auth Check")
    dot.node("D", "Dashboard")
    dot.node("E", "Error Page")
    dot.edge("A", "B")
    dot.edge("B", "C")
    dot.edge("C", "D", label="Valid")
    dot.edge("C", "E", label="Invalid")

    out_path = outdir / f"login_flow_graphviz.{fmt}"
    # graphviz .render expects a stem without extension
    stem = str(out_path.with_suffix(""))
    dot.render(stem, format=fmt, cleanup=True)
    print(f"[Graphviz] Wrote: {out_path}")
    return out_path

# ------------------------------------
# 2) MERMAID (mmdc if present; else proxy)
# ------------------------------------
MERMAID_SRC = """flowchart LR
    A[User] --> B[Login Page]
    B --> C{Auth Check}
    C -->|Valid| D[Dashboard]
    C -->|Invalid| E[Error Page]
"""

def render_mermaid(outdir: Path, fmt: str = "png") -> Path:
    mmd_file = outdir / "login_flow_mermaid.mmd"
    mmd_file.write_text(MERMAID_SRC, encoding="utf-8")
    print(f"[Mermaid] Wrote source: {mmd_file}")

    mmdc = shutil.which("mmdc")  # mermaid-cli
    if mmdc:
        out_file = outdir / f"login_flow_mermaid.{fmt}"
        cmd = [mmdc, "-i", str(mmd_file), "-o", str(out_file)]
        try:
            subprocess.run(cmd, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            print(f"[Mermaid] Rendered via mermaid-cli: {out_file}")
            return out_file
        except subprocess.CalledProcessError as e:
            print(f"[Mermaid] mmdc failed, falling back to proxy. Error:\n{e}", file=sys.stderr)

    # Fallback proxy image so you still get a visual
    try:
        import matplotlib.pyplot as plt
        import networkx as nx
        G = nx.DiGraph()
        G.add_edges_from([
            ("User", "Login Page"),
            ("Login Page", "Auth Check"),
            ("Auth Check", "Dashboard"),
            ("Auth Check", "Error Page"),
        ])
        pos = nx.spring_layout(G, seed=42)
        plt.figure(figsize=(6, 4))
        nx.draw(G, pos, with_labels=True, node_size=2500, font_size=9, font_weight="bold", arrows=True)
        out_file = outdir / f"login_flow_mermaid_proxy.{fmt}"
        plt.savefig(out_file)
        plt.close()
        print(f"[Mermaid] Proxy PNG written (install mermaid-cli for true render): {out_file}")
        return out_file
    except Exception as e:
        print(f"[Mermaid] Proxy render failed. You still have .mmd file: {mmd_file}\n{e}", file=sys.stderr)
        return mmd_file  # At least return the source path

# ------------------------------------
# 3) DIAGRAMS (true if installed; else proxy)
# ------------------------------------
def render_diagrams(outdir: Path, fmt: str = "png") -> Path:
    try:
        from diagrams import Diagram
        from diagrams.onprem.client import User as DUser
        from diagrams.onprem.compute import Server
        from diagrams.onprem.database import PostgreSQL

        # Diagrams outputs .png by default and uses Graphviz under the hood
        out_file_stem = outdir / "login_flow_diagrams"
        with Diagram("Login Flow", show=False, direction="LR", outformat=fmt, filename=str(out_file_stem)):
            user = DUser("End User")
            login = Server("Login Page")
            auth = Server("Auth Check")
            db = PostgreSQL("User DB")
            dashboard = Server("Dashboard")
            error = Server("Error Page")

            user >> login >> auth
            auth >> db
            auth >> dashboard
            auth >> error

        out_file = outdir / f"login_flow_diagrams.{fmt}"
        print(f"[Diagrams] Wrote: {out_file}")
        return out_file
    except Exception as e:
        print(f"[Diagrams] Library unavailable or failed ({e}). Falling back to proxy.", file=sys.stderr)
        # Proxy
        try:
            import matplotlib.pyplot as plt
            fig, ax = plt.subplots(figsize=(7, 4))
            ax.axis("off")
            boxes = {
                "User": (0.1, 0.5),
                "Login Page": (0.3, 0.5),
                "Auth Check": (0.5, 0.5),
                "Dashboard": (0.7, 0.7),
                "Error Page": (0.7, 0.3),
            }
            for text, (x, y) in boxes.items():
                ax.text(x, y, text, ha="center", va="center",
                        bbox=dict(boxstyle="round,pad=0.3", facecolor="white", edgecolor="black"))
            ax.annotate("", xy=boxes["Login Page"], xytext=boxes["User"], arrowprops=dict(arrowstyle="->"))
            ax.annotate("", xy=boxes["Auth Check"], xytext=boxes["Login Page"], arrowprops=dict(arrowstyle="->"))
            ax.annotate("Valid", xy=boxes["Dashboard"], xytext=boxes["Auth Check"], arrowprops=dict(arrowstyle="->"))
            ax.annotate("Invalid", xy=boxes["Error Page"], xytext=boxes["Auth Check"], arrowprops=dict(arrowstyle="->"))

            out_file = outdir / f"login_flow_diagrams_proxy.{fmt}"
            plt.savefig(out_file)
            plt.close()
            print(f"[Diagrams] Proxy PNG written: {out_file}")
            return out_file
        except Exception as e2:
            print(f"[Diagrams] Proxy render failed: {e2}", file=sys.stderr)
            return outdir

# ---------------------------
# main()
# ---------------------------
def main():
    import argparse
    parser = argparse.ArgumentParser(description="Generate Login Flow diagrams (Graphviz, Mermaid, Diagrams).")
    parser.add_argument("--outdir", default="out", help="Output directory (default: ./out)")
    parser.add_argument("--format", default="png", choices=["png", "svg", "pdf"], help="Output format (default: png)")
    args = parser.parse_args()

    outdir = Path(args.outdir).resolve()
    ensure_outdir(outdir)

    # Generate all three
    try:
        render_graphviz(outdir, fmt=args.format)
    except Exception as e:
        print(f"[Graphviz] Failed: {e}", file=sys.stderr)

    try:
        render_mermaid(outdir, fmt=args.format)
    except Exception as e:
        print(f"[Mermaid] Failed: {e}", file=sys.stderr)

    try:
        render_diagrams(outdir, fmt=args.format)
    except Exception as e:
        print(f"[Diagrams] Failed: {e}", file=sys.stderr)

    print("\nDone. Check your output folder:", outdir)

if __name__ == "__main__":
    main()
