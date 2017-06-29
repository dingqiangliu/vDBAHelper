/*
 * Copyright (c) 2006 - 2017, Hewlett-Packard Development Co., L.P. 
 * Description: parser for data from vertica logs.
 * Author: DingQiang Liu
 * Example: 
       make -C vsourceparser && python -c 'import vsourceparser; print vsourceparser.parseRows("18442240474082184385" + "\001" + "1.1" + "\001" + "-9223372036854775808" + "\001" + "true" + "\001"  + "one\xe4\xb8\xad\xe5\x9b\xbd" + "\002" + "2" + "\001" + "2.2" + "\001" + "544452155737558" + "\001" + "false" + "\001" + "two" + "\002" + "3" + "\001" + "3.3" + "\001" + "2016-12-31 03:00:01.123456" + "\001" + "TRUE" + "\001" + "three", ["integer", "double", "datetime", "boolean", "varchar"])'
 * Output: 
       [[-4503599627367231, 1.1, None, True, 'one'], [2, 2.2, '2017-04-02 20:42:35.737558', False, 'two'], [3, 3.3, '2016-12-31 03:00:01.123456', True, 'three']]
*/

#include <Python.h>

#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <time.h>

PyObject *parseValue(const char *sqltype, const char *value)
{
    // Till now, Vertica datacollector tables only use types: BOOLEAN, FLOAT, INTEGER, TIMESTAMP WITH TIME ZONE, VARCHAR
    if ((value == NULL) && !(strcmp(sqltype, "varchar") == 0 || strcmp(sqltype, "char") == 0))
    {
        return Py_None;
    }

    if (strcmp(sqltype, "integer") == 0 || strcmp(sqltype, "int") == 0 || strcmp(sqltype, "bigint") == 0 || strcmp(sqltype, "smallint") == 0 || strcmp(sqltype, "mediumint") == 0 || strcmp(sqltype, "tinyint") == 0 || strcmp(sqltype, "int2") == 0 || strcmp(sqltype, "int8") == 0)
    {
        // convert unsigned long to negative long. Note: INTEGER is numeric(18,0) in Vertica, eg. '18442240474082184385' means -4503599627367231
        unsigned long long lValue = strtoull(value, NULL, 10);
        return Py_BuildValue("l", (long long)lValue);
    }
    else if (strcmp(sqltype, "double") == 0 || strcmp(sqltype, "float") == 0 || strcmp(sqltype, "real") == 0 || strcmp(sqltype, "decimal") == 0 || strcmp(sqltype, "numeric") == 0)
    {
        double dValue = strtod(value, NULL);
        // Note: "1.1" will display "1.1" on macOS, but "1.1000000000000001" on RHEL6.5
        return Py_BuildValue("d", dValue);
    }
    else if (strcmp(sqltype, "date") == 0 || strcmp(sqltype, "datetime") == 0 || strcmp(sqltype, "timestamp") == 0)
    {
        char *pNext = NULL;
        long long lValue = strtoll(value, &pNext, 10);
        if (*pNext != '\0')
        {
            // value is string format
            return Py_BuildValue("s", value);
        }
        else if (lValue == -1 * 0x8000000000000000)
        {
            // -9223372036854775808(-0x8000000000000000) means null in Vertica
            return Py_None;
        }

        // 946684800 is secondes between '1970-01-01 00:00:00'(Python) and '2000-01-01 00:00:00'(Vertica)
        // format: datetime.fromtimestamp(float(lValue)/1000000+946684800).strftime("%Y-%m-%d %H:%M:%S.%f")
        time_t epoch = (time_t)(lValue / 1000000 + 946684800);
        long microsecond = lValue % 1000000;
        char sValue[19 + 1 + 6 + 1];
        memset(&sValue, 0, sizeof(sValue));
        strftime(sValue, sizeof(sValue), "%Y-%m-%d %H:%M:%S", localtime(&epoch));
        sprintf(sValue + 19, ".%06ld", microsecond);
        return Py_BuildValue("s", sValue);
    }
    else if (strcmp(sqltype, "boolean") == 0)
    {
        return (strncasecmp(value, "true", 4) == 0) ? Py_True : Py_False;
    }

    return Py_BuildValue("s", value);
}

static PyObject *vsource_parseRows(PyObject *self, PyObject *args)
{
    PyObject *rows = NULL;
    PyObject *columnTypes = NULL;

    if (!PyArg_ParseTuple(args, "OO", &rows, &columnTypes))
    {
        PyErr_SetString(PyExc_IndexError, "Invalid paramters!");
        return NULL;
    }
    if (rows == NULL || columnTypes == NULL)
    {
        PyErr_SetString(PyExc_IndexError, "Tow paramters need!");
        return NULL;
    }

    int columnSize = (int)PyList_Size(columnTypes);

    PyObject *listTable = PyList_New(0);

    char *p = PyString_AsString(rows);
    for (; *p != '\x0'; p++)
    {
        // new row/column
        PyObject *listRow = NULL;
        int colIndex = 0;
        char *pCol = p;

        for (; *p != '\x0'; p++)
        {
            if ((*p == '\x1') || (*p == '\x2') || (*(p + 1) == '\x0'))
            {
                // end of column
                // skip additional columns
                if (colIndex < columnSize)
                {
                    // backup seperator, set it to '\x0' as string terminator temporarily to avoid copy string
                    char *pSep = p;
                    if (*(p + 1) == '\x0')
                    {
                        pSep = p + 1;
                    }
                    char seperator = *pSep;
                    *pSep = '\x0';

                    PyObject *colType = PyList_GetItem(columnTypes, colIndex);
                    const char *colTypeName = PyString_AsString(colType);

                    if (listRow == NULL)
                    {
                        listRow = PyList_New(0);
                        PyList_Append(listTable, listRow);
                    }

                    PyList_Append(listRow, parseValue(colTypeName, pCol));

                    // restore seperator
                    *pSep = seperator;
                }
                // begin of next column
                pCol = p + 1;
                colIndex++;
            }

            if ((*p == '\x2') || (*(p + 1) == '\x0'))
            {
                // end of row
                break;
            }
        }
    }

    return listTable;
};

static PyMethodDef TimerMethods[] = {
    {"parseRows", vsource_parseRows, METH_VARARGS, "parse string format rows to list[list[object]."},
    {NULL, NULL, 0, NULL} /* Sentinel */
};

#if PY_MAJOR_VERSION >= 3
#define MOD_ERROR_VAL NULL
#define MOD_SUCCESS_VAL(val) val
#define MOD_INIT(name) PyMODINIT_FUNC PyInit_##name(void)
#define MOD_DEF(ob, name, doc, methods)                \
    static struct PyModuleDef moduledef = {            \
        PyModuleDef_HEAD_INIT, name, doc, -1, methods, \
    };                                                 \
    ob = PyModule_Create(&moduledef);
#else
#define MOD_ERROR_VAL
#define MOD_SUCCESS_VAL(val)
#define MOD_INIT(name) PyMODINIT_FUNC init##name(void)
#define MOD_DEF(ob, name, doc, methods) \
    ob = Py_InitModule3(name, methods, doc);
#endif

MOD_INIT(vsourceparser)
{
    PyObject *m;
    MOD_DEF(m, "vsourceparser", "parser for data from vertica logs.", TimerMethods)
    if (m == NULL)
        return MOD_ERROR_VAL;
    return MOD_SUCCESS_VAL(m);
}
