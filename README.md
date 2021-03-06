# sqlite3_ohlc_resample in C++
## In C++ using sqlite3, resample OHLC data into larger time-intervals, mimicking pandas' dataframe.resample

<b>License Note:</b> I chose "GNU Public something" in the repo-setup but I don't really know all the nuances and deltas from the other license-types. My intention is that anyone should be able to use this material, whether as-is (hah!) or as the basis for something else entirely.

<b>NEED:</b><br>
In C++ and given an sqlite database with some minute-based OHLC data, produce a table of OHLC data in 2-hour (or 5-minute, or 3-day, etc.) buckets. (Python and pandas make this super-easy, but I'm not USING python and pandas...)

<b>DISCUSSION:</b><br>
Solution came in two parts.
  First, a GROUP BY directive to make time-based buckets.
  Second, custom sqlite functions to get first (ie, "open") and last (ie, "close") values from each bucket.

Gratitude and props for the inspiration and guidance from <br>
http://souptonuts.sourceforge.net/readme_sqlite_tutorial.html <br>
https://www.sqlite.org/c3ref/value_blob.html <br>
https://stackoverflow.com/a/45729390 <br>

