***************
ExternalBiglist
***************

Creating a ExternalBiglist
=========================


Apache Parquet is a popular file format in the "big data" domain.
Many tools save large amounts of data in this format, often in a large number of files,
sometimes in nested directories.

:class:`ExternalBiglist` takes such data files as pre-existing, read-only, external data,
and provides an API to read the data in various ways.
This is analogous to, for example, the "external table" concept in BigQuery.

Let's create a couple small Parquet files to demonstrate this API.

>>> from cloudly.upathlib import LocalUpath
>>> import random
>>> from cloudly.biglist.parquet import write_arrays_to_parquet
>>> from cloudly.util.seq import Slicer
>>>
>>> path = LocalUpath('/tmp/a/b/c/e')
>>> path.rmrf()
0
>>> year = list(range(1970, 2021))
>>> make = ['honda'] * len(year)
>>> sales = list(range(123, 123 + len(make)))
>>> write_arrays_to_parquet([make, year, sales], path / 'honda.parquet', names=['make', 'year', 'sales'], row_group_size=10)
>>>
>>> year = list(range(1960, 2021))
>>> make = ['ford'] * len(year)
>>> sales = list(range(234, 234 + len(make)))
>>> write_arrays_to_parquet([make, year, sales], path / 'ford.parquet', names=['make', 'year', 'sales'], row_group_size=10)

Now we want to treat the contents of ``honda.parquet`` and ``ford.parquet`` combined as one dataset, and
use ``biglist`` tools to read it.

>>> from cloudly.biglist import ExternalBiglist
>>> car_data = ExternalBiglist.new(path, storage_format='parquet')
>>> car_data  # doctest: +SKIP
<ExternalBiglist at '/tmp/edd9cefb-179b-46d2-8946-7dc8ae1bdc50' with 112 records in 2 data file(s) stored at ['/tmp/a/b/c/e']>
>>> car_data.path  # doctest: +SKIP
LocalUpath('/tmp/edd9cefb-179b-46d2-8946-7dc8ae1bdc50')
>>> len(car_data)
112
>>> car_data.num_data_files
2
>>> list(car_data.files)
[<ParquetFileReader for '/tmp/a/b/c/e/ford.parquet'>, <ParquetFileReader for '/tmp/a/b/c/e/honda.parquet'>]

what :meth:`ExternalBiglist.new` does is to read the meta data of each file in the directory, recursively,
and save relevant info to facilitate its reading later.
The location given by ``car_data.path`` is the directory where :class:`ExternalBiglist` saves its meta info,
and not where the actual data are.
As is the case with :class:`Biglist`, this directory is a temporary one, which will be deleted once the object
``car_data`` goes away. If we wanted to keep the directory for future use, we should have specified a location
when calling :meth:`~ExternalBiglist.new`.


Reading an ExternalBiglist
==========================

The fundamental reading API is the same between :class:`Biglist` and :class:`ExternalBiglist`:
random access, slicing/dicing using :class:`Slicer`, iteration,
distributed reading via its :meth:`~ExternalBiglist.files`---these are all used the same way.

However, the structures of the data files are very different between :class:`Biglist` and :class:`ExternalBiglist`.
For Biglist, each data file contains a straight Python list, elements of which being whatever have been
passed into :meth:`Biglist.append`.
For ExternalBiglist, each data file is in a sophisticated columnar format, which is publicly documented.
A variety of ways are provided to get data out of the Parquet format;
some favor convenience, some others favor efficiency. Let's see some examples.

A row perspective
-----------------

>>> for i, x in enumerate(car_data):
...     print(x)
...     if i > 5:
...         break
{'make': 'ford', 'year': 1960, 'sales': 234}
{'make': 'ford', 'year': 1961, 'sales': 235}
{'make': 'ford', 'year': 1962, 'sales': 236}
{'make': 'ford', 'year': 1963, 'sales': 237}
{'make': 'ford', 'year': 1964, 'sales': 238}
{'make': 'ford', 'year': 1965, 'sales': 239}
{'make': 'ford', 'year': 1966, 'sales': 240}

This is the most basic iteration, :class:`Biglist`-style, one row (or "record") at a time.
When there are multiple columns, each row is presented as a dict with column names as keys.

Reading a Parquet data file is performed by :class:`ParquetFileReader`.

>>> f0 = car_data.files[0]
>>> f0
<ParquetFileReader for '/tmp/a/b/c/e/ford.parquet'>
>>> f0.path
LocalUpath('/tmp/a/b/c/e/ford.parquet')

First of all, a :class:`FileReader` object is a :class:`Seq`, providing row-based view into the data:

>>> len(f0)
61
>>> f0[2]
{'make': 'ford', 'year': 1962, 'sales': 236}
>>> f0[-10]
{'make': 'ford', 'year': 2011, 'sales': 285}
>>> Slicer(f0)[-3:].collect()
[{'make': 'ford', 'year': 2018, 'sales': 292}, {'make': 'ford', 'year': 2019, 'sales': 293}, {'make': 'ford', 'year': 2020, 'sales': 294}]
>>> for i, x in enumerate(f0):
...     print(x)
...     if i > 5:
...         break
{'make': 'ford', 'year': 1960, 'sales': 234}
{'make': 'ford', 'year': 1961, 'sales': 235}
{'make': 'ford', 'year': 1962, 'sales': 236}
{'make': 'ford', 'year': 1963, 'sales': 237}
{'make': 'ford', 'year': 1964, 'sales': 238}
{'make': 'ford', 'year': 1965, 'sales': 239}
{'make': 'ford', 'year': 1966, 'sales': 240}

:class:`ParquetFileReader` uses `pyarrow`_ to read the Parquet files.
The values above are nice and simple Python types, but they are not the original
pyarrow types;
they have undergone a conversion. This conversion can be toggled by the property
:data:`ParquetFileReader.scalar_as_py`:

>>> f0[8]
{'make': 'ford', 'year': 1968, 'sales': 242}
>>> f0.scalar_as_py = False
>>> f0[8]
{'make': <pyarrow.StringScalar: 'ford'>, 'year': <pyarrow.Int64Scalar: 1968>, 'sales': <pyarrow.Int64Scalar: 242>}
>>> f0.scalar_as_py = True

A Parquet file consists of one or more "row groups". Each row-group is a batch of rows stored column-wise.
We can get info about the row-groups, or even retrieve a row-group as the unit of processing:

>>> f0.num_row_groups
7
>>> f0.metadata  # doctest: +ELLIPSIS
<pyarrow._parquet.FileMetaData object at 0x7...>
  created_by: parquet-cpp-arrow version 1...
  num_columns: 3
  num_rows: 61
  num_row_groups: 7
  format_version: 2.6
  serialized_size: 23...
>>> f0.metadata.row_group(1)  # doctest: +ELLIPSIS
<pyarrow._parquet.RowGroupMetaData object at 0x7...>
  num_columns: 3
  num_rows: 10
  total_byte_size: 408
  sorting_columns: ()
>>> f0.metadata.row_group(0)  # doctest: +ELLIPSIS
<pyarrow._parquet.RowGroupMetaData object at 0x7...>
  num_columns: 3
  num_rows: 10
  total_byte_size: 408
  sorting_columns: ()
>>> rg = f0.row_group(0)
>>> rg
<ParquetBatchData with 10 rows, 3 columns>

(We have specified ``row_group_size=10`` in the call to :func:`write_arrays_to_parquet` for demonstration.
In practice, a row-group tends to be much larger.)

A :class:`ParquetBatchData` object is again a :class:`Seq`.
All of our row access tools are available:

>>> rg.num_rows
10
>>> len(rg)
10
>>> rg.num_columns
3
>>> rg[3]
{'make': 'ford', 'year': 1963, 'sales': 237}
>>> rg[-2]
{'make': 'ford', 'year': 1968, 'sales': 242}
>>> Slicer(rg)[4:7].collect()
[{'make': 'ford', 'year': 1964, 'sales': 238}, {'make': 'ford', 'year': 1965, 'sales': 239}, {'make': 'ford', 'year': 1966, 'sales': 240}]
>>> rg.scalar_as_py = False
>>> rg[3]
{'make': <pyarrow.StringScalar: 'ford'>, 'year': <pyarrow.Int64Scalar: 1963>, 'sales': <pyarrow.Int64Scalar: 237>}
>>> rg.scalar_as_py = True

When we request a specific row, :class:`ParquetFileReader` will load the row-group that contains the row of interest.
It doe not load the entire data in the file.
However, we can get greedy and ask for the whole data in one go:

>>> f0
<ParquetFileReader for '/tmp/a/b/c/e/ford.parquet'>
>>> f0.data()
<ParquetBatchData with 61 rows, 3 columns>

This, again, is a :class:`ParquetBatchData` object. All the familiar row access tools are at our disposal.

Finally, if the file is large, we may choose to iterate over it by batches instead of by rows:

>>> for batch in f0.iter_batches(batch_size=10):
...     print(batch)
<ParquetBatchData with 10 rows, 3 columns>
<ParquetBatchData with 10 rows, 3 columns>
<ParquetBatchData with 10 rows, 3 columns>
<ParquetBatchData with 10 rows, 3 columns>
<ParquetBatchData with 10 rows, 3 columns>
<ParquetBatchData with 10 rows, 3 columns>
<ParquetBatchData with 1 rows, 3 columns>

The batches are again :class:`ParquetBatchData` objects.
At the core of a ParquetBatchData is
a `pyarrow.Table`_
or `pyarrow.RecordBatch`_.
ParquetBatchData is friendly to `pickle <https://docs.python.org/3/library/pickle.html>`_ and,
I suppose, pickling `pyarrow`_ objects are very efficient.
So, the batches could be useful in `multiprocessing <https://docs.python.org/3/library/multiprocessing.html>`_ code.

A column perspective
--------------------

Parquet is a *columnar* format.
If we only need a subset of the columns, we should say so, so that the un-needed columns will
not be loaded from disk (or cloud, as it may be).

Both :class:`ParquetFileReader` and :class:`ParquetBatchData` provide the method :meth:`~ParquetFileReader.columns` 
(:meth:`~ParquetBatchData.columns`) to return a new object
with only the selected columns.
For ParquetFileReader, if data have not been loaded, reading of the new object will only load the selected columns.
For ParquetBatchData, its data is already in memory, hence column selection leads to a data subset.

>>> f0.column_names
['make', 'year', 'sales']
>>> cols = f0.columns(['year', 'sales'])
>>> cols
<ParquetFileReader for '/tmp/a/b/c/e/ford.parquet'>
>>> cols.num_columns
2
>>> cols.column_names
['year', 'sales']

:meth:`ParquetFileReader.columns` returns another :class:`ParquetFileReader`, whereas
:meth:`ParquetBatchData.columns` returns another :class:`ParquetBatchData`:

>>> rg
<ParquetBatchData with 10 rows, 3 columns>
>>> rg.column_names
['make', 'year', 'sales']
>>> rgcols = rg.columns(['make', 'year'])
>>> rgcols.column_names
['make', 'year']
>>> len(rgcols)
10
>>> rgcols[5]
{'make': 'ford', 'year': 1965}

It's an interesting case when there's only one column:

>>> f0
<ParquetFileReader for '/tmp/a/b/c/e/ford.parquet'>
>>> sales = f0.columns(['sales'])
>>> sales
<ParquetFileReader for '/tmp/a/b/c/e/ford.parquet'>
>>> sales.column_names
['sales']
>>> len(sales)
61
>>> sales[3]
237
>>> list(sales)  # doctest: +ELLIPSIS
[234, 235, 236, 237, 238, 239, ..., 291, 292, 293, 294]
>>> Slicer(sales)[:8].collect()
[234, 235, 236, 237, 238, 239, 240, 241]

Notice the type of the values (rows) returned from the element access methods: it's *not* ``dict``.
Because there's only one column whose name is known, there is no need to carry that info with every row.
Also note that the values have been converted to Python builtin types.
The original `pyarrow`_ values will not look as nice:
   
>>> sales.scalar_as_py = False
>>> Slicer(sales)[:8].collect()
[<pyarrow.Int64Scalar: 234>, <pyarrow.Int64Scalar: 235>, <pyarrow.Int64Scalar: 236>, <pyarrow.Int64Scalar: 237>, <pyarrow.Int64Scalar: 238>, <pyarrow.Int64Scalar: 239>, <pyarrow.Int64Scalar: 240>, <pyarrow.Int64Scalar: 241>]
>>> sales.scalar_as_py = True

Both :class:`ParquetFileReader` and :class:`ParquetBatchData` have another method called :meth:`~ParquetFileReader.column`
(:meth:`~ParquetBatchData.column`), which retrieves a single column
and returns a
`pyarrow.Array`_ or
`pyarrow.ChunkedArray`_. For example,

>>> sales2 = f0.column('sales')
>>> sales2  # doctest: +ELLIPSIS
<pyarrow.lib.ChunkedArray object at 0x...>
[
  [
    234,
    235,
    236,
    237,
    238,
    ...
    290,
    291,
    292,
    293,
    294
  ]
]

:meth:`ParquetFileReader.column` returns a 
`pyarrow.ChunkedArray`_, whereas
:meth:`ParquetBatchData.column` returns either a 
pyarrow.ChunkedArray or a 
`pyarrow.Array`_.


Performance considerations
--------------------------

While some ``biglist`` facilities shown here provide convenience and API elegance,
it may be a safe bet to use `pyarrow`_ facilities directly if ultimate performance is a requirement.

We have seen :data:`ParquetFileReader.scalar_as_py`
(and :data:`ParquetBatchData.scalar_as_py`); it's worthwhile to experiment whether that conversion impacts performance in a particular context.

There are several ways to get to a `pyarrow`_ object quickly and proceed with it.
A newly initiated :class:`ParquetFileReader` has not loaded any data yet.
Its property :data:`~ParquetFileReader.file` initiates a 
`pyarrow.parquet.ParquetFile`_ object (reading meta data during initiation)
and returns it. We may take it and go all the way down the `pyarrow`_ path:

>>> f1 = car_data.files[1]
>>> f1._data is None
True
>>> file = f1.file
>>> file  # doctest: +ELLIPSIS
<pyarrow.parquet.core.ParquetFile object at 0x7...>
>>> f1._file
<pyarrow.parquet.core.ParquetFile object at 0x7...>

We have seen that :meth:`ParquetFileReader.row_group` and :meth:`ParquetFileReader.iter_batches` both
return :class:`ParquetBatchData` objects. In contrast to :class:`ParquetFileReader`, which is "lazy" in terms of data loading,
a ParquetBatchData already has its data in memory. ParquetFileReader has another method,
namely :meth:`~ParquetFileReader.data`, that
eagerly loads the entire data of the file and wraps it in a ParquetBatchData object:

>>> data = f1.data()
>>> data
<ParquetBatchData with 51 rows, 3 columns>

The `pyarrow`_ data wrapped in :class:`ParquetBatchData` can be acquired easily:

>>> padata = data.data()
>>> padata
pyarrow.Table
make: string
year: int64
sales: int64
----
make: [["honda","honda","honda","honda","honda",...,"honda","honda","honda","honda","honda"]]
year: [[1970,1971,1972,1973,1974,...,2016,2017,2018,2019,2020]]
sales: [[123,124,125,126,127,...,169,170,171,172,173]]

Finally, we have seen that :meth:`ParquetFileReader.column` and :meth:`ParquetBatchData.column`---the single-column selectors---return
a `pyarrow`_ object. It is either a 
`pyarrow.Array`_ or a 
`pyarrow.ChunkedArray`_.


