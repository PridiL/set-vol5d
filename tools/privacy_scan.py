"""
Scan the generated dashboard for anything that shouldn't be published publicly.

Run before pushing to GitHub Pages: the page is self-contained, so whatever is
inside it becomes public along with it.
"""
import re
import sys
from pathlib import Path

DASHBOARD = Path(r"C:\Users\USER\Desktop\Claude Code\COT Data from Myfxbook\COT Dashboard.html")

CHECKS = {
    "Windows drive path": r"[A-Za-z]:[\\/]{1,2}Users",
    "username":           r"\bUSER\b",
    "email address":      r"[\w.+-]+@[\w-]+\.[\w.]+",
    "Desktop path":       r"Desktop[\\/]",
    "workbook filename":  r"MyFXBook\.xlsx",
    "local file:// link": r"file://",
    "author metadata":    r"""<meta[^>]*name=["']author""",
    "AI tooling mention": r"(?i)\b(claude|anthropic|openai|copilot)\b",
    "API key-ish string": r"(?i)(api[_-]?key|secret|token)\s*[:=]\s*['\"][^'\"]{8,}",
}


def main():
    path = Path(sys.argv[1]) if len(sys.argv) > 1 else DASHBOARD
    html = path.read_text(encoding="utf-8")
    # The embedded JSON is market data only; excluding it avoids false positives
    # from numbers that happen to match a pattern.
    body = re.sub(r"const DATA = \{.*?\};", "const DATA = {};", html, flags=re.S)

    print(f"Privacy scan: {path.name} ({path.stat().st_size/1024/1024:.2f} MB)\n")
    flagged = 0
    for label, pattern in CHECKS.items():
        hits = sorted(set(re.findall(pattern, body)))
        if hits:
            flagged += 1
            print(f"  [FLAG]  {label:20}: {hits[:3]}")
        else:
            print(f"  [clean] {label:20}")

    print()
    if flagged:
        print(f"{flagged} item(s) need review before publishing.")
        return 1
    print("Nothing personal, local, or credential-like is embedded. Safe to publish.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
