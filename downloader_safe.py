"""Compatibility wrapper for the main downloader app.

This project historically carried a second copy of the GUI downloader logic in
`downloader_safe.py`. That duplicate had drifted and become syntactically
broken, which made basic validation fail. To keep the compatibility entrypoint
without maintaining two diverging implementations, this module now delegates to
the primary implementation in `downloader.py`.
"""

from downloader import *  # noqa: F401,F403


def main():
    import downloader as _downloader

    _downloader.main()


if __name__ == "__main__":
    main()
