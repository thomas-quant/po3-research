"""Backward-compatible entrypoint for PO3 research.

The implementation lives in `po3_research.research`. Keep this wrapper so
existing commands (`python3 analysis.py`) and imports (`import analysis`) work.
"""

from po3_research import research as _research

globals().update({name: value for name, value in vars(_research).items() if name != "__name__"})


def main():
    return _research.main()


if __name__ == "__main__":
    main()
