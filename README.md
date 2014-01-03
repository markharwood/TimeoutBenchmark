# Patches for benchmarking search timeout logic


## Overview

This modified version of the benchmarking framework (see https://code.google.com/a/apache-extras.org/p/luceneutil/) 
expects to find an "ActivityTimeMonitor" class in both of the Lucene versions it compares.



### Lucene patches
The "base no-op" version of ActivityTimeMonitor should be added to the relevant directory containing the base version of Lucene used in benchmarking.
It adds no extra logic and only serves to satisfy a new compile-time dependency in the benchmarking framework.

The classes in the "real impl" directory provide the real over-run detection logic and should be
added to the directory containing a copy of the base Lucene code which is then used for benchmark comparisons

### Benchmark framework patches
The "perfPatches" directory includes a slightly modified version of these benchmark classes:

1) SearchTask - the "go" method adds calls to start and stop a time monitor

2) OpenDirectory - a new "timedWrapper" method wraps the choice of Lucene Directory only 
   if "TimeLimitedDirectory" is a class available in the Lucene version.
   
### Running tests
The usual benchmarking tests can be run using:

    python localrun.py
    
and output should show a small (max 10%) reduction in search performance for the new version with over-run detection logic.
