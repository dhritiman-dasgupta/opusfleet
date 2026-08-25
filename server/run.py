"""Role dispatcher -- one image, one entrypoint, many services.

    python -m server.run ingest | segmenter | levels | live | api | sink
"""

import sys

ROLES = ("ingest", "segmenter", "levels", "live", "api", "sink")


def main(argv=None):
    argv = list(sys.argv[1:] if argv is None else argv)
    role = argv[0] if argv else "ingest"
    if role not in ROLES:
        print(f"unknown role {role!r}; expected one of: {', '.join(ROLES)}")
        return 2

    if role == "ingest":
        from . import ingest as m
    elif role == "segmenter":
        from . import segmenter as m
    elif role == "levels":
        from . import levels as m
    elif role == "live":
        from . import live as m
    elif role == "api":
        from . import api as m
    else:
        from . import sink as m
        return m.main(argv[1:])

    m.run()
    return 0


if __name__ == "__main__":
    sys.exit(main())
