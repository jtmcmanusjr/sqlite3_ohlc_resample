// ConTest.cpp : Demonstrates C++  OHLC "resample" in sqlite3
//
// Given some minute-based OHLC data, produce a table of
//   OHLC data in 2-hour (or 5-minute, or 3-day, etc.) buckets.
//   (Python and pandas make this super-easy, but I'm not USING
//   python and pandas...)
//
// Solution came in two parts.
//   First, a GROUP BY directive to make time-based buckets.
//   Second, custom sqlite functions to get first (ie, "open") and 
//      last (ie, "close") values from each bucket.
//
// Gratitude and props for the inspiration and guidance from
// http://souptonuts.sourceforge.net/readme_sqlite_tutorial.html
// https://www.sqlite.org/c3ref/value_blob.html
// https://stackoverflow.com/a/45729390
//

#include "stdafx.h"
#include <random>
#include <string>

#include "sqlite3.h"

// Custom functions implemented here are
//   First_S (first value, as a string)
//   Last_S  (last value, as a string)
// Custom functions are implemented as two sub-functions
//   'Step' func that processes a value from each row in each time-bucket.
//   'Finalize' func that reports the processed value for each time-bucket.
// Here, each custom func has its own Step-func, but they share a Finalize func.


// This structure is used by both First_S and Last_S functions.
typedef struct FirstLastStringHolder FirstLastStringHolder;
struct FirstLastStringHolder {
	std::string sVal;     /* the string value */
	int cnt;        /* Number of elements encountered */
};


static void FirstStep_S(sqlite3_context *context, int argc, sqlite3_value **argv) {
	FirstLastStringHolder *p = NULL;
	char tmp[32];

	if (argc != 1) return;

	p = (FirstLastStringHolder *)sqlite3_aggregate_context(context, sizeof(*p));
	if (p->cnt == 0)    /* When zero first time through */
	{
		p->sVal = (char *)sqlite3_value_text(argv[0]);
	}
	else {
		// We only care about the first.
	}
	p->cnt++;
}

static void LastStep_S(sqlite3_context *context, int argc, sqlite3_value **argv) {
	FirstLastStringHolder *p = NULL;

	if (argc != 1) return;

	// Always store the most-recently-encountered value.
	p = (FirstLastStringHolder *)sqlite3_aggregate_context(context, sizeof(*p));
	p->sVal = (char *)sqlite3_value_text(argv[0]);
}

static void FirstLastFinalize_S(sqlite3_context *context) {
	FirstLastStringHolder *p = NULL;
	char *buf = NULL;
	p = (FirstLastStringHolder *)sqlite3_aggregate_context(context, sizeof(*p));

	buf = (char *)malloc(sizeof(char)*(p->sVal.length() + 2));
	if (buf == NULL)
		fprintf(stderr, "malloc error in FirstFinalize, buf\n");

	snprintf(buf, p->sVal.length() + 1, "%s", p->sVal.c_str());
	sqlite3_result_text(context, buf, p->sVal.length() + 1, free);
}



sqlite3 *InitializeSqlite3DB(const char*sFile)
{
	sqlite3 *db = NULL;
	int rc;

	rc = sqlite3_open(sFile, &db);
	if (rc) {
		fprintf(stderr, "Can't open database: %s\n", sqlite3_errmsg(db));
		return(NULL);
	}

	// Incorporate custom functions
	if (sqlite3_create_function(db, "First_S", 1, SQLITE_UTF8, NULL, NULL, &FirstStep_S,
		&FirstLastFinalize_S) != 0)
		fprintf(stderr, "Problem with S using FirstStep_S and FirstLastFinalize_S\n");

	if (sqlite3_create_function(db, "Last_S", 1, SQLITE_UTF8, NULL, NULL, &LastStep_S,
		&FirstLastFinalize_S) != 0)
		fprintf(stderr, "Problem with S using LastStep and FirstLastFinalize_S\n");

	return db;
}



// Callback to process results from SELECT exec.
// Prints rows of CSV to console.
static int callback_csv2console(void *data, int argc, char **argv, char **azColName) {
	int i;
	static bool bBeenHere = false;

	if (!bBeenHere)
	{
		bBeenHere = true;
		for (i = 0; i < argc-1; i++) {
			printf("\"%s\",", azColName[i]);
		}
		printf("\"%s\"\n", azColName[i]);
	}

	for (i = 0; i<argc-1; i++) {
		printf("\"%s\",", argv[i] ? argv[i] : "NULL");
	}
	printf("\"%s\"\n", argv[i] ? argv[i] : "NULL");
	return 0;
}


int main()
{
	// Demo sqlite3 custom functions and OHLC resample
	printf("\nsqlite\n");
	sqlite3 *db;
	char *zErrMsg = 0;
	int rc;
	
	char *sFile = "./testData_offset.db";
	const char* data = "Callback function called";

	// This sets up the magic, implemneted in DatabaseHelpers.cpp
	db = InitializeSqlite3DB(sFile);
	if (!db) {
		printf("Database failure!");
		return 0;
	}

	// Run OHLC resampling SQL using First_S and Last_S
	// "120 * 60" in the GROUP BY says "resample using 120-minute buckets"
	// NOTE, the "dateX" column is of type TIMESTAMP!
	char *sql = " SELECT min(dateX) as date, max(high) as high, min(low) as low, sum(volume) as volume "  \
			" , First_S(open) as open " \
			" , Last_S(close) as close " \
			" FROM  testTable_offset "  \
			" GROUP BY strftime('%s', dateX) / (120 * 60);";
	printf("sql: %s\n", sql);
	rc = sqlite3_exec(db, sql, callback_csv2console, (void *)NULL, &zErrMsg);

	if (rc != SQLITE_OK) {
		fprintf(stderr, "SQL error: %s\n", zErrMsg);
		sqlite3_free(zErrMsg);
	}
	else {
		fprintf(stdout, "Operation done successfully\n");
	}

	sqlite3_close(db);

	// MS Visual Studio launches a new console window for the run.
	// I do this to keep it around until I'm done viewing it.
	printf("\nENTER to exit: ");
	char sIn[8];
	gets_s(sIn, 8);

	return 0;
}

