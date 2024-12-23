
.. testsetup:: *

   import cloudly.biglist
   from cloudly.biglist import *
   from cloudly.upathlib import LocalUpath

.. testcleanup::

   LocalUpath('/tmp/a/b/c').rmrf()



*******
biglist
*******



Creating a Biglist
==================

Create a new :class:`Biglist` object via the classmethod :meth:`~Biglist.new`:

>>> from cloudly.biglist import Biglist
>>> mylist = Biglist.new(batch_size=100)

then add data to it, for example,

>>> for x in range(10_023):
...     mylist.append(x)

This saves a new data file for every 100 elements
accumulated. In the end, there are 23 elements in a memory buffer that are
not yet persisted to disk. The code has no way to know whether we will append
more elements soon, hence it does not save this *partial* batch.
Suppose we're done with adding data, we call :meth:`~Biglist.flush` to persist
the content of the buffer to disk:

>>> mylist.flush()

If, after a while, we decide to append more data to ``mylist``, we just call :meth:`~Biglist.append` again.
We can continue to add more data as long as the disk has space.
New data files will be saved. The smaller file containing 23 elements will stay there
among larger files with no problem.

Now let's take a look at the biglist object:

>>> mylist  # doctest: +SKIP
<Biglist at '/tmp/19f88a17-3e78-430f-aad0-a35d39485f80' with 10023 elements in 101 data file(s)>
>>> len(mylist)
10023
>>> mylist.path  # doctest: +SKIP
LocalUpath('/tmp/19f88a17-3e78-430f-aad0-a35d39485f80')
>>> mylist.num_data_files
101

The data have been saved in the directory ``/tmp/19f88a17-3e78-430f-aad0-a35d39485f80``,
which is a temporary one because we did not tell :meth:`~Biglist.new` where to save data.
When the object ``mylist`` gets garbage collected, this directory will be deleted automatically.
This has its uses, but often we want to save the data for future use. In that case, just pass 
a currently non-existent directory to :meth:`~Biglist.new`, for example,

>>> yourlist = Biglist.new('/tmp/project/data/store-a', batch_size=10_000)

Later, initiate a :class:`Biglist` object for reading the existing dataset:

>>> yourlist = Biglist('/tmp/project/data/store-a')

.. doctest::
   :hide:

   >>> yourlist.destroy()

If we want to persist the data in Google Cloud Storage, we would specify a path in the
``'gs://bucket-name/path/to/data'`` format.


The Seq protocol and FileReader class
=====================================

Before going further with Biglist, let's digress a bit and introduce a few helper facilities.

:class:`BiglistBase`
(and its subclasses :class:`Biglist` and :class:`ParquetBiglist`)
could have implemented the |Sequence|_ interface in the standard library.
However, that interface contains a few methods that are potentially hugely inefficient for Biglist,
and hence are not supposed to be used on a Biglist.
These methods include ``__contains__``, ``count``, and ``index``.
These methods require iterating over the entire dataset for purposes about one particular data item.
For Biglist, this would require loading and unloading each of a possibly large number of data files.
Biglist does not want to give user the illusion that they can use these methods at will and lightly.

For this reason, the protocol :class:`Seq` is defined, which has three methods:
:meth:`~Seq.__len__`, :meth:`~Seq.__iter__`, and :meth:`~Seq.__getitem__`.
Therefore, classes that implement this protocol are
`Sized <https://docs.python.org/3/library/collections.abc.html#collections.abc.Sized>`_,
`Iterable <https://docs.python.org/3/library/collections.abc.html#collections.abc.Iterable>`_,
and support random element access by index. Most classes in the ``biglist`` package
implement this protocol rather than the standard |Sequence|_.

Because a biglist manages any number of data files, a basic operation concerns reading one data file.
Each subclass of :class:`BiglistBase` implements its file-reading class as a subclass of
:class:`FileReader`. FileReader implements the :class:`Seq` protocol, hence
the data items in one data file can be used like a list.
Importantly, a FileReader instance does not load the data file upon initialization.
At that moment, the instance can be *pickled*. This lends itself to uses in multiprocessing.
This point of the design will be showcased later.

The centerpiece of a biglist is a sequence of data files in persistence, or correspondingly,
a sequence of FileReader's in memory. The property :meth:`BiglistBase.files`
returns a ``FileSeq`` to manage the FileReader objects of the biglist.

Finally, :class:`BiglistBase`
implements the :class:`Seq` protocol for its data items across the data files.

To sum up,
a ``BiglistBase`` is a ``Seq`` of data items across data files;
``BiglistBase.files`` is a ``FileSeq``, which in turn is a ``Seq`` of ``FileReader``\s;
a ``FileReader`` is a ``Seq`` of data items in one data file.



Reading a Biglist
=================

Random element access
---------------------

We can access any element of a :class:`Biglist` like we do a list:

>>> mylist[18]
18
>>> mylist[-3]
10020

Biglist does not support slicing directly.
However, the class :class:`Slicer` wraps a :class:`Seq` and enables element access by a single index, by a slice, or by a list of indices:

>>> from cloudly.util.seq import Slicer
>>> v = Slicer(mylist)
>>> len(v)
10023
>>> v  # doctest: +SKIP
<Slicer into 10023/10023 of <Biglist at '/tmp/dc260854-8041-40e8-801c-34084451d7a3' with 10023 elements in 101 data file(s)>>
>>> v[83]
83
>>> v[100:104]  # doctest: +SKIP
<Slicer into 4/10023 of <Biglist at '/tmp/dc260854-8041-40e8-801c-34084451d7a3' with 10023 elements in 101 data file(s)>>
>>>

Note that slicing the slicer does not return a list of values.
Instead, it returns another :class:`Slicer` object, which, naturally, can be used the same way,
including slicing further.

A :class:`Slicer` object is a |Iterable|_ (in fact, it is a :class:`Seq`),
hence we can gather all of its elements in a list:

>>> list(v[100:104])
[100, 101, 102, 103]

:class:`Slicer` provides a convenience method :meth:`~Slicer.collect` to do the same:

>>> v[100:104].collect()
[100, 101, 102, 103]


A few more examples:

>>> v[-8:].collect()
[10015, 10016, 10017, 10018, 10019, 10020, 10021, 10022]
>>> v[-8::2].collect()
[10015, 10017, 10019, 10021]
>>> v[[1, 83, 250, -2]]  # doctest: +SKIP
<Slicer into 4/10023 of <Biglist at '/tmp/dc260854-8041-40e8-801c-34084451d7a3' with 10023 elements in 101 data file(s)>>
>>> v[[1, 83, 250, -2]].collect()
[1, 83, 250, 10021]
>>> v[[1, 83, 250, -2]][-3:].collect()
[83, 250, 10021]


Iteration
---------

Don't be carried away by the many easy and flexible ways of random access.
Random element access for :class:`Biglist` is **inefficient**.
The reason is that it needs to load a data file that contains the element of interest.
If the biglist has many data files and we are "jumping around" randomly,
it is wasting a lot of time loading entire files just to access a few data elements in them.
(However, *consecutive* random accesses to elements residing in the same file will not load the file
repeatedly.)

The preferred way to consume the data of a :class:`Biglist` is to iterate over it. For example,

>>> for i, x in enumerate(mylist):
...     print(x)
...     if i > 4:
...         break
0
1
2
3
4
5

Conceptually, this loads each data file in turn and yields the elements in each file.
The implementation "pre-loads" a few files in background threads to speed up the iteration.


Reading from a Biglist in multiple processes
--------------------------------------------

To *collectively* consume a :class:`Biglist` object from multiple processes,
we can distribute :class:`FileReader`\s to the processes.
The FileReader's of ``mylist`` is accessed via its property :meth:`~Biglist.files`, which returns a ``FileSeq``:

>>> files = mylist.files
>>> files  # doctest: +SKIP
<BiglistFileSeq at '/tmp/dc260854-8041-40e8-801c-34084451d7a3' with 10023 elements in 101 data file(s)>
>>> len(files)
101
>>> files.num_data_files
101
>>> files.num_data_items
10023
>>> files[0]  # doctest: +SKIP
<BiglistFileReader for '/tmp/cfb39dc0-94bb-4557-a056-c7cea20ea653/store/1669667946.647939_46eb97f6-bdf3-45d2-809c-b90c613d69c7_100.pickle_zstd'>

A :class:`FileReader` object is light-weight. Upon initialization, it has not loaded the file yet---it merely records the file path along with the function that will be used to load the file.
In addition, FileReader objects are friendly to pickling, hence lend themselves to multiprocessing code.
Let's design a small experiment to consume this dataset in multiple processes:

>>> def worker(file_reader):
...     total = 0
...     for x in file_reader:
...         total += x
...     return total
>>> from concurrent.futures import ProcessPoolExecutor
>>> total = 0
>>> with ProcessPoolExecutor(5) as pool:  # doctest: +SKIP
...     tasks = [pool.submit(worker, fr) for fr in mylist.files]  # doctest: +SKIP
...     for t in tasks:  # doctest: +SKIP
...         total += t.result()  # doctest: +SKIP
>>> total  # doctest: +SKIP
50225253

.. using the ProcessPoolExecutor in a context manager causes it to hang;
.. see https://stackoverflow.com/questions/48218897/python-doctest-hangs-using-processpoolexecutor

What is the expected result?

>>> sum(mylist)
50225253

Sure enough, this verifies that the entire biglist is consumed by the processes *collectively*.

If the file loading is the bottleneck of the task, we can use threads in place of processes.

Similarly, it is possible to read ``mylist`` from multiple machines if ``mylist`` is stored
in the cloud. Since a FileReader object is pickle-able, it works just fine if we pickle it
and send it to another machine, provided the file path that is contained in the FileReader object
is in the cloud, hence accessible from the other machine.
We need a mechanism to distribute these FileReader objects to machines.
For that, check out ``Multiplexer`` from ``upathlib``.


Writing to a Biglist in multiple workers
========================================

The flip side of distributed reading is distributed writing.
If we have a biglist on the local disk, we can append to it from multiple processes or threads.
If we have a biglist in the cloud, we can append to it from multiple machines.
Let's use multiprocessing to demo the idea.

First, we create a new :class:`Biglist` at a storage location of our choosing:

>>> from cloudly.upathlib import LocalUpath
>>> path = LocalUpath('/tmp/a/b/c/d')
>>> path.rmrf()
0
>>> yourlist = Biglist.new(path, batch_size=6)

In each worker process, we will open this biglist by ``Biglist(path)`` and append data to it.
Now that this has a presence on the disk, ``Biglist(path)`` will not complain the dataset does not exist.

>>> yourlist.info
{'storage_format': 'pickle-zstd', 'storage_version': 3, 'batch_size': 6, 'data_files_info': []}
>>> yourlist.path
LocalUpath('/tmp/a/b/c/d')
>>> len(yourlist)
0

Then we can tell workers, "here is the location, add data to it." Let's design a simple worker:

>>> def worker(path, idx):
...     yourlist = Biglist(path)
...     for i in range(idx):
...         yourlist.append(100 * idx + i)
...     yourlist.flush()

From the main process, let's instruct the workers to write data to the same :class:`Biglist`:

>>> import multiprocessing
>>> with ProcessPoolExecutor(10, mp_context=multiprocessing.get_context('spawn')) as pool:  # doctest: +SKIP
...     tasks = [pool.submit(worker, path, idx) for idx in range(10)]  # doctest: +SKIP
...     for t in tasks:  # doctest: +SKIP
...         _ = t.result()  # doctest: +SKIP

Let's see what we've got:


>>> yourlist.reload()  # doctest: +SKIP
>>> len(yourlist)  # doctest: +SKIP
45
>>> yourlist.num_data_files  # doctest: +SKIP
12
>>> list(yourlist)  # doctest: +SKIP
[400, 401, 402, 403, 500, 501, 502, 503, 504, 600, 601, 602, 603, 604, 605, 700, 701, 702, 703, 704, 705, 706, 900, 901, 902, 903, 904, 905, 906, 907, 908, 100, 800, 801, 802, 803, 804, 805, 806, 807, 200, 201, 300, 301, 302]
>>>

Does this look right? It took me a moment to realize that ``idx = 0`` did not append anything.
So, the data elements are in the 100, 200, ..., 900 ranges; that looks right.
But the order of things is confusing.

Well, in a distributed setting, there's no guarantee of order.
It's not a problem that numbers in the 800 range come *after* those in the 900 range.

We can get more insights if we dive to the file level:

>>> for f in yourlist.files:  # doctest: +SKIP
...     print(f)  # doctest: +SKIP
<BiglistFileReader for '/tmp/a/b/c/d/store/20230129073410.971439_f84d0cf3-e2c4-40a7-acf2-a09296ff73bc_1.pickle_zstd'>
<BiglistFileReader for '/tmp/a/b/c/d/store/20230129073410.973651_63e4ca6d-4e44-49e1-a035-6d60a88f7789_.pickle_zstd'>
<BiglistFileReader for '/tmp/a/b/c/d/store/20230129073410.975576_f59ab2f0-be9c-477d-a95b-70d3dfc00d94_6.pickle_zstd'>
<BiglistFileReader for '/tmp/a/b/c/d/store/20230129073410.982828_3219d2d1-50e2-4b41-b595-2c6df4e63d3c_6.pickle_zstd'>
<BiglistFileReader for '/tmp/a/b/c/d/store/20230129073410.983024_674e57de-66ed-4e3b-bb73-1db36c13fd6f_1.pickle_zstd'>
<BiglistFileReader for '/tmp/a/b/c/d/store/20230129073410.985425_78eec966-8139-4401-955a-7b81fb8b47b9_6.pickle_zstd'>
<BiglistFileReader for '/tmp/a/b/c/d/store/20230129073410.985555_752b4975-fbf3-4172-9063-711722a83abc_3.pickle_zstd'>
<BiglistFileReader for '/tmp/a/b/c/d/store/20230129073411.012161_3a7620f5-b040-4cec-9018-e8bd537ea98d_1.pickle_zstd'>
<BiglistFileReader for '/tmp/a/b/c/d/store/20230129073411.034502_4a340751-fa1c-412e-8f49-13f2ae83fc3a_6.pickle_zstd'>
<BiglistFileReader for '/tmp/a/b/c/d/store/20230129073411.035010_32c58dbe-e3a2-4ba1-9ffe-32c127df11a6_2.pickle_zstd'>
<BiglistFileReader for '/tmp/a/b/c/d/store/20230129073411.067370_20a0e926-7a5d-46a1-805d-86d16c346852_2.pickle_zstd'>
<BiglistFileReader for '/tmp/a/b/c/d/store/20230129073411.119890_89ae31bc-7c48-488d-8dd1-e22212773d79_3.pickle_zstd'>

The file names do not appear to be totally random. They follow some pattern that facilitates ordering, and they have encoded some useful info.
In fact, the part before the first underscore is the date and time of file creation, with a resolution to microseconds.
This is followed by a `uuid.uuid4 <https://docs.python.org/3/library/uuid.html#uuid.uuid4>`_ random string.
When we iterate the :class:`Biglist` object, files are read in the order of their paths, hence in the order of creation time.
The number in the file name before the suffix is the number of elements in the file.

We can get similar info in a more readable format:

>>> for v in yourlist.files.data_files_info:  # doctest: +SKIP
...     print(v)  # doctest: +SKIP
['/tmp/a/b/c/d/store/20230129073410.971439_f84d0cf3-e2c4-40a7-acf2-a09296ff73bc_4.pickle_zstd', 4, 4]
['/tmp/a/b/c/d/store/20230129073410.973651_63e4ca6d-4e44-49e1-a035-6d60a88f7789_5.pickle_zstd', 5, 9]
['/tmp/a/b/c/d/store/20230129073410.975576_f59ab2f0-be9c-477d-a95b-70d3dfc00d94_6.pickle_zstd', 6, 15]
['/tmp/a/b/c/d/store/20230129073410.982828_3219d2d1-50e2-4b41-b595-2c6df4e63d3c_6.pickle_zstd', 6, 21]
['/tmp/a/b/c/d/store/20230129073410.983024_674e57de-66ed-4e3b-bb73-1db36c13fd6f_1.pickle_zstd', 1, 22]
['/tmp/a/b/c/d/store/20230129073410.985425_78eec966-8139-4401-955a-7b81fb8b47b9_6.pickle_zstd', 6, 28]
['/tmp/a/b/c/d/store/20230129073410.985555_752b4975-fbf3-4172-9063-711722a83abc_3.pickle_zstd', 3, 31]
['/tmp/a/b/c/d/store/20230129073411.012161_3a7620f5-b040-4cec-9018-e8bd537ea98d_1.pickle_zstd', 1, 32]
['/tmp/a/b/c/d/store/20230129073411.034502_4a340751-fa1c-412e-8f49-13f2ae83fc3a_6.pickle_zstd', 6, 38]
['/tmp/a/b/c/d/store/20230129073411.035010_32c58dbe-e3a2-4ba1-9ffe-32c127df11a6_2.pickle_zstd', 2, 40]
['/tmp/a/b/c/d/store/20230129073411.067370_20a0e926-7a5d-46a1-805d-86d16c346852_2.pickle_zstd', 2, 42]
['/tmp/a/b/c/d/store/20230129073411.119890_89ae31bc-7c48-488d-8dd1-e22212773d79_3.pickle_zstd', 3, 45]

The values for each entry are file path, number of elements in the file, and accumulative number of elements.
The accumulative count is obviously the basis for random access---:class:`Biglist` uses this to
figure out which file contains the element at a specified index.



API reference
=============


.. autoclass:: cloudly.biglist.FileReader


.. autoclass:: cloudly.biglist._base.BiglistBase


.. autoclass:: cloudly.biglist.Biglist


.. autoclass:: cloudly.biglist.BiglistFileReader


.. autoclass:: cloudly.biglist.ParquetBiglist


