.. upathlib documentation master file, created by
   sphinx-quickstart on Fri Nov 25 22:11:50 2022.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

.. testsetup:: *

   from cloudly.upathlib import LocalUpath

.. testcleanup::

   LocalUpath('/tmp/abc').rmrf()



********
upathlib
********


.. automodule:: cloudly.upathlib
   :no-members:
   :no-undoc-members:
   :no-special-members:



Quickstart
==========

Let's carve out a space in the local file system and poke around.

>>> from cloudly.upathlib import LocalUpath
>>> p = LocalUpath('/tmp/abc')

This creates a :class:`~cloudly.upathlib.LocalUpath` object ``p`` that points to the location
``'/tmp/abc'``. This may be an existing file, or directory, or may be nonexistent.
We know this is a temporary location; to be sure we have a clear playground, let's
wipe out anything and everything:

>>> p.rmrf()
0

Think ``rm -rf /tmp/abc``. It does just that. The returned `0` means zero files were deleted.

Now let's create a file and write something to it:

>>> (p / 'x.txt').write_text('first')

This creates file ``/tmp/abc/x.txt`` with the content ``'first'``. Note the directory ``'/tmp/abc'``
did not exist before the call. We did not need to "create the parent directory".
In fact, ``upathlib`` does not provide a way to do that.
In ``upathlib``, "directory" is a "virtual" thing that is embodied by a group of files.
For example, if there exist

::

    /tmp/abc/x.txt
    /tmp/abc/d/y.data

we say there is directories ``'/tmp/abc'`` and ``'/tmp/abc/d'``, but we
don't create these "directories" by themselves. These directories come into being
if there exist such files.

Let's actually create these files:

>>> (p / 'x.txt').write_text('second', overwrite=True)
>>> (p / 'd' / 'y.data').write_bytes(b'0101')

Now let's look into this directory:

>>> p.is_dir()
True
>>> (p / 'd').is_dir()
True
>>> (p / 'x.txt').is_dir()
False
>>> (p / 'x.txt').is_file()
True

We can navigate in the directory. For example,

>>> for v in sorted(p.iterdir()):  # the sort merely makes the result stable
...     print(v)
/tmp/abc/d
/tmp/abc/x.txt

This is only the first level, or "direct children". We can also use "recursive iterdir"
to get all files under the directory, descending into subdirectories recursively:

>>> for v in sorted(p.riterdir()):  # the sort merely makes the result stable
...     print(v)
/tmp/abc/d/y.data
/tmp/abc/x.txt

This time only *files* are listed. Subdirectories do not show up because,
after all, they are *not real* in ``upathlib`` concept.

We can as easily read a file, like

>>> (p / 'x.txt').read_text()
'second'

Several common file formats are provided out of the box, including
text, bytes, json, and pickle, as well as compressed versions by 
`zlib <https://www.zlib.net/>`_ and 
`Zstandard <http://facebook.github.io/zstd/>`_.

Let's do some JSON:

>>> pp = p / 'e/f/g/data.json'
>>> pp.write_json({'name': 'John', 'age': 38})

We know the JSON file is also a text file, so we can treat it as such:

>>> pp.read_text()
'{"name": "John", "age": 38}'

But usually we prefer to get back the Python object directly:

>>> v = pp.read_json()
>>> v
{'name': 'John', 'age': 38}
>>> type(v)
<class 'dict'>

We can go "down" the directory tree using ``/``.
Conversely, we can go "up" using :meth:`~upathlib.Upath.parent`:

>>> pp.path
PosixPath('/tmp/abc/e/f/g/data.json')
>>> pp.parent
LocalUpath('/tmp/abc/e/f/g')
>>> pp.parent.parent
LocalUpath('/tmp/abc/e/f')
>>> pp.parent.parent.is_dir()
True
>>> pp.parent.parent.is_file()
False

or the terminal-lovers' ``..``:

>>> pp
LocalUpath('/tmp/abc/e/f/g/data.json')
>>> pp / '..'
LocalUpath('/tmp/abc/e/f/g')
>>> pp / '..' / '..'
LocalUpath('/tmp/abc/e/f')

Under the hood, ``/`` delegates to a call to :meth:`~upathlib.Upath.joinpath`:

>>> pp.joinpath('../../o/p/q')
LocalUpath('/tmp/abc/e/f/o/p/q')

Let's see again what we have:

>>> sorted(p.riterdir())
[LocalUpath('/tmp/abc/d/y.data'), LocalUpath('/tmp/abc/e/f/g/data.json'), LocalUpath('/tmp/abc/x.txt')]

and to get rid of them all:

>>> p.rmrf()
3

A nice thing about ``upathlib`` is the "unified" nature across local and cloud storages.
Suppose we have set up the environment to use Google Cloud Storage, then we could have started this excercise with


>>> from cloudly.gcp.storage import GcsBlobUpath
>>> p = GcsBlobUpath('gs://my-bucket/tmp/abc')

Everything after this would work unchanged. (The printouts would be different at some places, 
e.g. :class:`LocalUpath` would be replaced by :class:`GcsBlobUpath`.)


Upath
=====

.. automodule:: cloudly.upathlib._upath
    :no-members:
    :no-undoc-members:
    :no-special-members:

.. autoclass:: cloudly.upathlib.Upath

.. autoclass:: cloudly.upathlib.FileInfo


LocalUpath
==========

.. automodule:: cloudly.upathlib._local
    :no-members:
    :no-undoc-members:
    :no-special-members:


.. autoclass:: cloudly.upathlib.LocalUpath


BlobUpath
=========

.. automodule:: cloudly.upathlib._blob



Two particular applications of ``upathlib`` are ``multiplexer`` and ``VersionedUplodable``.
