"""Backward-compatible entrypoint for PO3 research.

The implementation lives in `po3_research.research`. Keep this wrapper so
existing commands (`python3 analysis.py`) and imports (`import analysis`) work.
"""

from po3_research import research as _research

# Re-export the research API without clobbering this module's own identity
# (__file__/__doc__/__spec__ belong to analysis.py, not to research.py).
_MODULE_METADATA = {"__name__", "__doc__", "__file__", "__spec__", "__loader__", "__package__", "__builtins__", "__path__", "__cached__"}
globals().update({name: value for name, value in vars(_research).items() if name not in _MODULE_METADATA})


def main(argv: list = None):
    return _research.main(argv)


if __name__ == "__main__":
    main()
