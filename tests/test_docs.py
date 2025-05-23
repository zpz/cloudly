import subprocess
from pathlib import Path


def test_docs():
    p = Path(__file__)
    p_docs = p.parent.parent / 'docs'
    print('\n... running doctest ...')
    subprocess.run(['make', 'doctest'], cwd=p_docs, check=True)  # noqa: S603, S607
    print('\n... building documentation ...')
    subprocess.run(  # noqa: S603
        ['make', 'html', 'SPHINXOPTS=-W'],  # noqa: S603, S607
        cwd=p_docs,
        check=True,  # noqa: S603, S607
    )  # noqa: S603, S607
    # If build fails, this will raise exception.
